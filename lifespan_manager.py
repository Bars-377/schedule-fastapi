import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal

import aiohttp
import aiomysql
from models import AsyncSessionLocal, BranchData, Branche, Metric, engine
from recalc import recalc
from sqlmodel import SQLModel, select

# ==============================
# --- Настройка логирования ---
# ==============================
logger = logging.getLogger("branch_update")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# ==============================
# --- Конфигурация ---
# ==============================
with open("config.json", encoding="utf-8") as f:
    config = json.load(f)

editing_metric_names = [n.lower() for n in config.get("editing_metrics", [])]

BITRIX_BASE_URL = "https://bitrix.mfc.tomsk.ru/rest/533/dfk26tp3grjqm2b4"
BITRIX_USER_LIST_URL = f"{BITRIX_BASE_URL}/user.get.json?ADMIN_MODE=True&SORT=ID&ORDER=ASC&start={{start}}"
BITRIX_USER_INFO_URL = f"{BITRIX_BASE_URL}/user.get.json?id={{user_id}}"
BITRIX_DEPARTMENT_URL = f"{BITRIX_BASE_URL}/department.get.json?ID={{dept_id}}"

UPDATE_HOUR = 10
UPDATE_MINUTE = 00

MYSQL_CONFIG = {
    "host": config["mysql"]["host"],
    "user": config["mysql"]["user"],
    "password": config["mysql"]["password"],
    "db": config["mysql"]["database"],
    "charset": config["mysql"]["charset"],
}

mysql_pool: aiomysql.Pool | None = None
mysql_connected: bool = False  # Явное состояние

# ==============================
# --- Повтор с обработкой ошибок ---
# ==============================
async def retry_forever(func, *args, delay: int = 5, name: str = "unknown", **kwargs):
    attempt = 1
    while True:
        try:
            # logger.info(f"[{name}] Попытка {attempt} выполнения функции...")
            # logger.info(f"[{name}] Успешно выполнено после {attempt} попыток")
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"[{name}] ❌ Ошибка: {e}. Повтор через {delay} сек (попытка {attempt})",
                exc_info=True
            )
            await asyncio.sleep(delay)
            attempt += 1


# ==============================
# --- MySQL ---
# ==============================
async def init_mysql_pool(timeout: int = 10) -> aiomysql.Pool:
    global mysql_pool, mysql_connected

    if mysql_connected and mysql_pool is not None:
        return mysql_pool

    while True:
        try:
            logger.info("Подключение к MySQL...")
            mysql_pool = await asyncio.wait_for(
                aiomysql.create_pool(
                    minsize=1,
                    maxsize=10,
                    **MYSQL_CONFIG
                ),
                timeout=timeout
            )
            mysql_connected = True
            logger.info("✅ Успешное подключение к MySQL")
            return mysql_pool
        except TimeoutError:
            logger.error(f"⏱ Таймаут подключения к MySQL ({timeout} сек)")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к MySQL: {e}", exc_info=True)
        await asyncio.sleep(5)


async def fetch_absences_for_user_async(employee_id: int):
    # logger.info(f"Запрос отсутствий для сотрудника ID={employee_id}")
    pool = await init_mysql_pool()
    query = """
        SELECT
            e.ID AS ELEMENT_ID,
            e.NAME AS ABSENCE_NAME,
            e.ACTIVE_FROM,
            e.ACTIVE_TO,
            p_user.VALUE AS EMPLOYEE_ID,
            TRIM(CONCAT(u.LAST_NAME, ' ', u.NAME, ' ', IFNULL(u.SECOND_NAME, ''))) AS EMPLOYEE_NAME,
            enum_type.VALUE AS ABSENCE_TYPE
        FROM b_iblock_element e
        LEFT JOIN b_iblock_element_property p_user
            ON e.ID = p_user.IBLOCK_ELEMENT_ID AND p_user.IBLOCK_PROPERTY_ID = 1
        LEFT JOIN b_user u
            ON u.ID = CAST(p_user.VALUE AS UNSIGNED)
        LEFT JOIN b_iblock_element_property p_type
            ON e.ID = p_type.IBLOCK_ELEMENT_ID AND p_type.IBLOCK_PROPERTY_ID = 4
        LEFT JOIN b_iblock_property_enum enum_type
            ON enum_type.ID = p_type.VALUE_ENUM
        WHERE e.IBLOCK_ID = 1 AND p_user.VALUE = %s
        ORDER BY e.ACTIVE_FROM;
    """

    async def _fetch():
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query, (employee_id,))
                    return await cursor.fetchall()
        except aiomysql.OperationalError as e:
            logger.warning(f"MySQL OperationalError: {e}. Попробуем переподключиться.")
            # Обнуляем состояние, чтобы при следующем вызове init_mysql_pool переподключился
            global mysql_connected, mysql_pool
            mysql_connected = False
            mysql_pool = None
            raise  # retry_forever перехватит и повторит запрос

    return await retry_forever(_fetch, name=f"MySQL fetch_absences ID={employee_id}")


# ==============================
# --- Bitrix ---
# ==============================
async def fetch_json(session, url: str, timeout: int = 10):
    async def _fetch():
        async with session.get(url, timeout=timeout) as resp:
            data = await resp.json()
            # logger.info(f"Bitrix-запрос {url} завершён, получено {len(data.get('result', []))} элементов")
            return data.get("result", [])
    return await retry_forever(_fetch, name=f"Bitrix {url}")

async def fetch_departments_from_bitrix(session):
    logger.info("Загрузка списка отделов из Bitrix...")
    return await fetch_json(session, f"{BITRIX_BASE_URL}/department.get.json")

async def fetch_users_from_bitrix(session):
    logger.info("Загрузка списка пользователей из Bitrix...")
    all_users, start, page_size = [], 0, 50
    while True:
        result = await fetch_json(session, BITRIX_USER_LIST_URL.format(start=start))
        if not result:
            break
        all_users.extend(result)
        start += page_size
    logger.info(f"Всего загружено пользователей: {len(all_users)}")
    return all_users


async def fetch_user_info(session, employee_id):
    # logger.info(f"Загрузка информации о пользователе ID={employee_id}")
    return await fetch_json(session, BITRIX_USER_INFO_URL.format(user_id=employee_id))

async def fetch_department_info(session, dept_id):
    result = await fetch_json(session, BITRIX_DEPARTMENT_URL.format(dept_id=dept_id))
    # logger.info(f"Информация о департаменте {dept_id}: {dept_info}")
    return result[0]["ID"].strip() if result else None


# ==============================
# --- BranchData обновление ---
# ==============================
async def update_branchdata_value(branchdata: BranchData, value: float, log_prefix: str = ""):
    old_value = branchdata.value
    branchdata.value = Decimal(value)
    logger.info(f"{log_prefix} Обновлено значение BranchData (branch_id={branchdata.branch_id}, metric_id={branchdata.metric_id}): {old_value} -> {value}")


# ==============================
# --- Филиалы и метрики ---
# ==============================
async def update_branches(db, departments, metrics):
    global editing_metric_names

    today = date.today()

    for dept in departments:
        name = dept.get("NAME", "").strip()
        department_id = int(dept.get("ID"))
        # logger.info(f"Обработка филиала: {name} (ID={department_id})")

        stmt = select(Branche).where(Branche.department_id == department_id)
        branch = (await db.execute(stmt)).scalar_one_or_none()
        if not branch:
            branch = Branche(name=name, department_id=department_id)
            db.add(branch)
            await db.flush()
            logger.info(f"✅ Добавлен новый филиал: {name} (ID={department_id})")

        # Создаём BranchData
        for metric in metrics:
            stmt_check = select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == today,
            )
            existing_record = (await db.execute(stmt_check)).scalar_one_or_none()

            if not existing_record:
                if metric.name.lower() in editing_metric_names:
                    stmt_prev = select(BranchData).where(
                        BranchData.branch_id == branch.id,
                        BranchData.metric_id == metric.id,
                        BranchData.record_date < today,
                    ).order_by(BranchData.record_date.desc()).limit(1)
                    prev_record = (await db.execute(stmt_prev)).scalar_one_or_none()
                    value = prev_record.value if prev_record else Decimal("0.00")
                else:
                    value = Decimal("0.00")
                branchdata = BranchData(branch_id=branch.id, metric_id=metric.id, record_date=today, value=value)
                db.add(branchdata)
                # logger.info(f"➕ Создан BranchData для филиала {name}, metric {metric.name} -> {value}")

        # Пересчёт метрик
        branchdata_list = (await db.execute(
            select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.record_date == today
            ).order_by(BranchData.metric_id)
        )).scalars().all()
        for bd in branchdata_list:
            # logger.info(f"Пересчёт метрики branch_id={bd.branch_id}, metric_id={bd.metric_id}")
            await recalc(bd, db, branchdata_list)

    await db.commit()


async def _load_metrics_from_db():
    """Загружает метрики и возвращает словарь имя_метрики -> id."""
    async with AsyncSessionLocal() as db:
        metrics = (await db.execute(select(Metric))).scalars().all()
    return {m.name.lower(): m.id for m in metrics}


def _check_missing_metrics(metric_map, metric_names):
    """Проверяет, какие метрики отсутствуют, и логирует предупреждение."""
    missing = [name for name in metric_names.values() if not metric_map.get(name)]
    if missing:
        logger.warning(f"Не найдены метрики в таблице Metric: {', '.join(missing)}")


async def _process_single_user(user, session, metric_ids, sick_leaves, all_vacations, special_users, semaphore):
    """Обрабатывает одного пользователя и обновляет соответствующие словари."""
    employee_id = int(user["ID"])

    today = date.today()

    async with semaphore:
        absences = await fetch_absences_for_user_async(employee_id)
        user_info_result = await fetch_user_info(session, employee_id)

    if not user_info_result:
        return

    user_info = user_info_result[0]
    is_special_user = user_info.get("UF_USR_1759203471311") == "105"

    dept_id = None
    if "UF_DEPARTMENT" in user_info and user_info["UF_DEPARTMENT"]:
        dept_id_raw = user_info["UF_DEPARTMENT"][0]
        async with semaphore:
            dept_id = await fetch_department_info(session, dept_id_raw)
    if not dept_id:
        return

    # --- Спецпользователи ---
    if is_special_user and metric_ids["special"]:
        special_users.setdefault(dept_id, {}).setdefault(metric_ids["special"], set()).add(employee_id)

    # --- Отпуск / больничный ---
    for absence in absences:
        absence_type = absence["ABSENCE_TYPE"]
        active_from = datetime.strptime(str(absence["ACTIVE_FROM"]), "%Y-%m-%d %H:%M:%S").date()
        active_to = datetime.strptime(str(absence["ACTIVE_TO"]), "%Y-%m-%d %H:%M:%S").date()
        if not (active_from <= today <= active_to):
            continue

        if absence_type == "больничный" and metric_ids["sick"]:
            sick_leaves.setdefault(dept_id, {}).setdefault(metric_ids["sick"], set()).add(employee_id)
        elif absence_type in (
            "отпуск ежегодный",
            "отпуск декретный",
            "отпуск без сохранения заработной платы",
        ) and metric_ids["vacation"]:
            all_vacations.setdefault(dept_id, {}).setdefault(metric_ids["vacation"], set()).add(employee_id)
        elif not absence_type and metric_ids["absence"]:
            all_vacations.setdefault(dept_id, {}).setdefault(metric_ids["absence"], set()).add(employee_id)


async def process_vacations(session, users):
    """
    Получает данные об отсутствии сотрудников и возвращает словари dept_id -> {metric_id: set(employee_ids)}.
    Даже если сотрудников нет, метрики будут присутствовать с пустым множеством.
    """

    # --- 1. Загружаем конфиг метрик ---
    metric_config = config.get("metrics", {})
    metric_names = {
        "special": metric_config.get("special", "").lower(),
        "sick": metric_config.get("sick", "").lower(),
        "vacation": metric_config.get("vacation", "").lower(),
        "absence": metric_config.get("absence", "").lower(),
    }

    # --- 2. Загружаем метрики из БД ---
    metric_map = await _load_metrics_from_db()
    _check_missing_metrics(metric_map, metric_names)

    metric_ids = {
        "special": metric_map.get(metric_names["special"]),
        "sick": metric_map.get(metric_names["sick"]),
        "vacation": metric_map.get(metric_names["vacation"]),
        "absence": metric_map.get(metric_names["absence"]),
    }

    # --- 3. Инициализация структур для всех департаментов и метрик ---
    all_vacations = {}
    sick_leaves = {}
    special_users = {}
    semaphore = asyncio.Semaphore(10)

    # Создаём пустые множества для всех метрик
    async with AsyncSessionLocal() as db:
        departments = (await db.execute(select(Branche.department_id))).scalars().all()
        for dept_id in departments:
            for key, mid in metric_ids.items():
                if mid is None:
                    continue
                if key == "sick":
                    sick_leaves.setdefault(dept_id, {})[mid] = set()
                else:
                    all_vacations.setdefault(dept_id, {})[mid] = set()
            for key, mid in metric_ids.items():
                if key == "special" and mid:
                    special_users.setdefault(dept_id, {})[mid] = set()

    # --- 4. Обработка пользователей ---
    async with asyncio.TaskGroup() as tg:
        for user in users:
            tg.create_task(_process_single_user(
                user, session, metric_ids, sick_leaves, all_vacations, special_users, semaphore
            ))

    return sick_leaves, all_vacations, special_users


async def update_vacations(db, departments_employees):
    """
    Обновляет BranchData. Если нет сотрудников для метрики, значение обнуляется.
    """
    today = date.today()
    for dept_id, metrics_dict in departments_employees.items():
        stmt_branch = select(Branche).where(Branche.department_id == int(dept_id))
        branch = (await db.execute(stmt_branch)).scalar_one_or_none()
        if not branch:
            logger.warning(f"Филиал для department_id={dept_id} не найден")
            continue
        logger.info(f"Обновление метрик филиала {branch.name} (department_id={dept_id})")

        for metric_id, employees_set in metrics_dict.items():
            value = len(employees_set) if employees_set else 0  # если пусто, обнуляем
            stmt_data = select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == metric_id,
                BranchData.record_date == today,
            )
            bd = (await db.execute(stmt_data)).scalar_one_or_none()
            if bd:
                await update_branchdata_value(bd, value, log_prefix=f"{branch.name}")
            else:
                bd = BranchData(branch_id=branch.id, metric_id=metric_id, record_date=today, value=value)
                db.add(bd)
                logger.info(f"➕ Добавлена новая запись BranchData: {branch.name}, metric_id={metric_id} -> {value}")
    await db.commit()


async def get_or_create_virtual_branch(db, virtual_department_id: int = 99, name: str = "АУП"):
    stmt = select(Branche).where(Branche.department_id == virtual_department_id)
    branch = (await db.execute(stmt)).scalar_one_or_none()
    if not branch:
        branch = Branche(name=name, department_id=virtual_department_id)
        db.add(branch)
        await db.commit()
        await db.refresh(branch)
    return branch


async def schedule_update_loop():
    await asyncio.sleep(3)  # небольшая задержка перед первым запуском
    while True:
        now = datetime.now()
        target_time = now.replace(
            hour=UPDATE_HOUR, minute=UPDATE_MINUTE, second=0, microsecond=0
        )
        if now >= target_time:
            target_time += timedelta(days=1)
        wait_seconds = (target_time - now).total_seconds()
        logger.info(f"Следующее обновление через {wait_seconds / 3600:.2f} ч.")
        await asyncio.sleep(wait_seconds)

        async with aiohttp.ClientSession() as session:
            # --- 1. Загружаем данные из Bitrix ---
            departments = await fetch_departments_from_bitrix(session)
            users = await fetch_users_from_bitrix(session)

            # --- 2. Обновляем обычные филиалы ---
            async with AsyncSessionLocal() as db:
                metrics = (await db.execute(select(Metric))).scalars().all()
                await update_branches(db, departments, metrics)

            # --- 3. Обрабатываем отпуска, больничные и спецпользователей ---
            sick_leaves, all_vacations, special_users = await process_vacations(session, users)

            async with AsyncSessionLocal() as db:
                await update_vacations(db, sick_leaves)
                await update_vacations(db, all_vacations)
                await update_vacations(db, special_users)

            # --- 4. Создаём/обновляем виртуальный филиал "АУП" ---
            async with AsyncSessionLocal() as db:
                ids_aup = (1, 31, 2, 29, 28, 15, 21, 4, 25, 26, 27, 24, 3, 23, 16, 20, 61, 17, 18)
                await ensure_virtual_branch(db, ids_aup)
                logger.info("✅ Виртуальный филиал 'АУП' создан/обновлён")



# ==============================
# --- Обновление виртуального филиала "АУП" ---
# ==============================
async def ensure_virtual_branch(db, ids_aup: tuple[int], virtual_department_id: int = 99, name: str = "АУП"):
    global editing_metric_names

    today_date = date.today()

    # --- 1. Создаём или получаем виртуальный филиал ---
    virtual_branch = await get_or_create_virtual_branch(db, virtual_department_id, name)

    # --- 2. Загружаем все метрики ---
    metrics = (await db.execute(select(Metric))).scalars().all()

    # --- 3. Загружаем все филиалы ids_aup ---
    stmt_branches = select(Branche).where(Branche.department_id.in_(ids_aup))
    branches = (await db.execute(stmt_branches)).scalars().all()
    branch_map = {b.department_id: b for b in branches}

    # --- 4. Загружаем BranchData для этих филиалов и сегодняшней даты ---
    stmt_branchdata = select(BranchData).where(
        BranchData.branch_id.in_([b.id for b in branches]),
        BranchData.record_date == today_date
    )
    branchdata_list = (await db.execute(stmt_branchdata)).scalars().all()

    # --- 5. Строим словарь branch_id -> metric_id -> value ---
    branch_data_map: dict[int, dict[int, Decimal]] = {}
    for bd in branchdata_list:
        branch_data_map.setdefault(bd.branch_id, {})[bd.metric_id] = bd.value

    # --- 6. Формируем BranchData для виртуального филиала ---
    virtual_branchdata_list = []
    for metric in metrics:
        if metric.name.lower() in editing_metric_names:
            # Для editing_metrics берем значение с предыдущей даты
            stmt_prev = select(BranchData).where(
                BranchData.metric_id == metric.id,
                BranchData.branch_id == virtual_branch.id,
                BranchData.record_date < today_date
            ).order_by(BranchData.record_date.desc()).limit(1)
            prev_bd = (await db.execute(stmt_prev)).scalar_one_or_none()
            value = prev_bd.value if prev_bd else Decimal("0.00")
        else:
            # Суммируем по всем филиалам ids_aup из словаря. Если данных нет, будет 0
            value = sum(
                branch_data_map.get(branch.id, {}).get(metric.id, Decimal("0.00"))
                for branch in branch_map.values()
            )

        # Проверяем, есть ли уже запись для виртуального филиала
        stmt_check = select(BranchData).where(
            BranchData.branch_id == virtual_branch.id,
            BranchData.metric_id == metric.id,
            BranchData.record_date == today_date
        )
        bd = (await db.execute(stmt_check)).scalar_one_or_none()
        if not bd:
            bd = BranchData(branch_id=virtual_branch.id, metric_id=metric.id, record_date=today_date, value=value)
            db.add(bd)
        else:
            bd.value = value  # Обновляем значение, даже если оно стало 0
        virtual_branchdata_list.append(bd)

    await db.commit()

    # --- 7. Пересчёт метрик виртуального филиала ---
    for bd in virtual_branchdata_list:
        await recalc(bd, db, virtual_branchdata_list)


# ==============================
# --- Lifespan FastAPI ---
# ==============================
@asynccontextmanager
async def lifespan(app):
    logger.info(f"Сервер запущен в {datetime.now():%Y-%m-%d %H:%M:%S}")

    task = None
    if config.get("enable_background_task", True):
        logger.info("✅ Фоновая задача включена")

        # Инициализация MySQL пула и создание таблиц
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

        async def background_tasks():
            try:
                await init_mysql_pool()
            except Exception as e:
                logger.error(f"Не удалось инициализировать MySQL: {e}", exc_info=True)
                return

            while True:
                try:
                    await schedule_update_loop()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Фоновое обновление завершилось с ошибкой: {e}", exc_info=True)
                    await asyncio.sleep(30)  # ждём перед повтором

        task = asyncio.create_task(background_tasks())
    else:
        logger.info("⚠️ Фоновая задача отключена")

    try:
        yield
    finally:
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if mysql_pool:
            mysql_pool.close()
            await mysql_pool.wait_closed()
        await engine.dispose()

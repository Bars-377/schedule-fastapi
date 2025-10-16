import asyncio
import json
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

import aiohttp
import aiomysql
from models import AsyncSessionLocal, BranchData, Branche, Metric, engine
from recalc import recalc
from sqlmodel import SQLModel, select

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==============================
# --- Конфигурация ---
# ==============================
with open("config.json", encoding="utf-8") as f:
    config = json.load(f)

BITRIX_BASE_URL = "https://bitrix.mfc.tomsk.ru/rest/533/dfk26tp3grjqm2b4"
BITRIX_USER_LIST_URL = f"{BITRIX_BASE_URL}/user.get.json?ADMIN_MODE=True&SORT=ID&ORDER=ASC&start={{start}}"
BITRIX_USER_INFO_URL = f"{BITRIX_BASE_URL}/user.get.json?id={{user_id}}"
BITRIX_DEPARTMENT_URL = f"{BITRIX_BASE_URL}/department.get.json?ID={{dept_id}}"

UPDATE_HOUR = 10
UPDATE_MINUTE = 00

today = date.today()

MYSQL_CONFIG = {
    "host": config["mysql"]["host"],
    "user": config["mysql"]["user"],
    "password": config["mysql"]["password"],
    "db": config["mysql"]["database"],
    "charset": config["mysql"]["charset"],
}

# ==============================
# --- Глобальный пул MySQL ---
# ==============================
mysql_pool: aiomysql.Pool | None = None


# ==============================
# --- Повтор с обработкой ошибок ---
# ==============================
async def retry_forever(
    func, *args, delay: int = 5, name: str = "unknown", **kwargs
):
    attempt = 1
    while True:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"[{datetime.now():%Y-%m-%d %H:%M:%S}] ❌ Ошибка при работе \
                    с {name}: {e}. Повтор через {delay} сек (попытка {attempt})", exc_info=True
            )
            await asyncio.sleep(delay)
            attempt += 1


# ==============================
# --- Работа с MySQL ---
# ==============================
async def init_mysql_pool(timeout: int = 10):
    """
    Инициализация глобального пула MySQL с ограничением времени подключения.
    """
    global mysql_pool
    if mysql_pool and not mysql_pool._closed:
        return mysql_pool

    while True:
        try:
            logger.info("Подключение к MySQL...")
            # asyncio.wait_for ограничивает время await
            mysql_pool = await asyncio.wait_for(
                aiomysql.create_pool(**MYSQL_CONFIG), timeout=timeout
            )
            logger.info("Успешное подключение к MySQL")
            return mysql_pool
        except TimeoutError:
            logger.error(f"Таймаут подключения к MySQL ({timeout} сек)")
        except Exception as e:
            logger.error(
                f"[{datetime.now():%Y-%m-%d %H:%M:%S}] ❌ Ошибка подключения к MySQL: {e}. Повтор через 5 сек...", exc_info=True
            )

        # Ждём перед следующей попыткой
        await asyncio.sleep(5)


async def fetch_absences_for_user_async(employee_id: int):
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
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (employee_id,))
                return await cursor.fetchall()

    return await retry_forever(_fetch, name="MySQL fetch_absences")


# ==============================
# --- Работа с Bitrix ---
# ==============================
async def fetch_json(session, url: str, timeout: int = 10):
    async def _fetch():
        async with session.get(url, timeout=timeout) as resp:
            data = await resp.json()
            return data.get("result", [])

    return await retry_forever(_fetch, name=f"Bitrix {url}")


async def fetch_departments_from_bitrix(session):
    url = f"{BITRIX_BASE_URL}/department.get.json"
    return await fetch_json(session, url)


async def fetch_users_from_bitrix(session):
    all_users, start, page_size = [], 0, 50
    while True:
        result = await fetch_json(
            session, BITRIX_USER_LIST_URL.format(start=start)
        )
        if not result:
            break
        all_users.extend(result)
        start += page_size
    return all_users


async def fetch_user_info(session, employee_id):
    return await fetch_json(
        session, BITRIX_USER_INFO_URL.format(user_id=employee_id)
    )


async def fetch_department_info(session, dept_id):
    result = await fetch_json(
        session, BITRIX_DEPARTMENT_URL.format(dept_id=dept_id)
    )
    return result[0]["ID"].strip() if result else None


# ==============================
# --- Обновление данных ---
# ==============================
async def update_branches(db, departments, metrics):
    """
    Обновляет филиалы и создаёт записи BranchData за текущую дату.
    Редактируемые метрики берут значение из предыдущей даты.
    Для всех новых записей выполняется recalc.
    """
    # Список редактируемых метрик из config.json
    editing_metric_names = [name.lower() for name in config.get("editing_metrics", [])]

    for dept in departments:
        name = dept.get("NAME", "").strip()
        department_id = int(dept.get("ID"))

        stmt = select(Branche).where(Branche.department_id == department_id)
        branch = (await db.execute(stmt)).scalar_one_or_none()
        if not branch:
            branch = Branche(name=name, department_id=department_id)
            db.add(branch)
            await db.commit()
            await db.refresh(branch)
            logger.info(f"✅ Добавлен филиал: {name}")

        # --- 1. Создаём BranchData за текущую дату для всех метрик ---
        for metric in metrics:
            stmt_check = select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == today,
            )
            existing_record = (await db.execute(stmt_check)).scalar_one_or_none()

            if not existing_record:
                if metric.name.lower() in editing_metric_names:
                    stmt_prev = (
                        select(BranchData)
                        .where(
                            BranchData.branch_id == branch.id,
                            BranchData.metric_id == metric.id,
                            BranchData.record_date < today,
                        )
                        .order_by(BranchData.record_date.desc())
                        .limit(1)
                    )
                    prev_record = (await db.execute(stmt_prev)).scalar_one_or_none()
                    value = prev_record.value if prev_record else 0.0
                else:
                    value = 0.0

                branchdata = BranchData(
                    branch_id=branch.id,
                    metric_id=metric.id,
                    record_date=today,
                    value=Decimal(value),
                )
                db.add(branchdata)

        # --- Сохраняем все новые записи перед recalc ---
        await db.commit()

        # --- 2. Пересчёт всех метрик филиала за текущую дату ---
        branchdata_list = (await db.execute(
            select(BranchData)
            .where(BranchData.branch_id == branch.id, BranchData.record_date == today)
            .order_by(BranchData.metric_id)
        )).scalars().all()

        for bd in branchdata_list:
            await recalc(bd, db)


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


async def process_vacations(session, users):
    """
    Получает данные об отсутствии сотрудников и возвращает словари dept_id -> {metric_id: set(employee_ids)}.
    """

    # --- 1. Загружаем конфиг ---
    metric_config = config.get("metrics", {})
    metric_names = {
        "special": metric_config.get("special", "").lower(),
        "sick": metric_config.get("sick", "").lower(),
        "vacation": metric_config.get("vacation", "").lower(),
    }

    # --- 2. Загружаем метрики ---
    metric_map = await _load_metrics_from_db()
    _check_missing_metrics(metric_map, metric_names)

    metric_ids = {
        "special": metric_map.get(metric_names["special"]),
        "sick": metric_map.get(metric_names["sick"]),
        "vacation": metric_map.get(metric_names["vacation"]),
    }

    # --- 3. Инициализация структур ---
    all_vacations = {}
    sick_leaves = {}
    special_users = {}
    semaphore = asyncio.Semaphore(10)

    # --- 4. Обработка пользователей ---
    await asyncio.gather(*(
        _process_single_user(u, session, metric_ids, sick_leaves, all_vacations, special_users, semaphore)
        for u in users
    ))

    return sick_leaves, all_vacations, special_users


async def update_vacations(db, departments_employees):
    for dept_id, metrics_dict in departments_employees.items():
        stmt_branch = select(Branche).where(
            Branche.department_id == int(dept_id)
        )
        branch = (await db.execute(stmt_branch)).scalar_one_or_none()
        if not branch:
            logger.warning(f"Филиал для department_id={dept_id} не найден")
            continue

        for metric_id, employees_set in metrics_dict.items():
            stmt_data = select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == metric_id,
                BranchData.record_date == today,
            )
            branch_data = (await db.execute(stmt_data)).scalar_one_or_none()

            if branch_data:
                branch_data.value = len(employees_set)
                logger.info(
                    f"🔄 Обновлено значение (metric {metric_id}): {branch.name} ({len(employees_set)})"
                )
            else:
                db.add(
                    BranchData(
                        branch_id=branch.id,
                        metric_id=metric_id,
                        record_date=today,
                        value=len(employees_set),
                    )
                )
                logger.info(
                    f"✅ Добавлена новая запись (metric {metric_id}): {branch.name} ({len(employees_set)})"
                )

    await db.commit()


async def schedule_update_loop():
    await asyncio.sleep(3)
    while True:
        now = datetime.now()
        target_time = now.replace(
            hour=UPDATE_HOUR, minute=UPDATE_MINUTE, second=0, microsecond=0
        )
        if now >= target_time:
            target_time += timedelta(days=1)
        wait_seconds = (target_time - now).total_seconds()
        logger.info(
            f"Следующее обновление через {wait_seconds / 3600:.2f} ч."
        )
        await asyncio.sleep(wait_seconds)

        async with aiohttp.ClientSession() as session:
            departments = await fetch_departments_from_bitrix(session)
            users = await fetch_users_from_bitrix(session)

            async with AsyncSessionLocal() as db:
                metrics = (await db.execute(select(Metric))).scalars().all()
                await update_branches(db, departments, metrics)

            # print(session)
            # print(users)

            sick_leaves, all_vacations, special_users = await process_vacations(session, users)

            async with AsyncSessionLocal() as db:
                await update_vacations(db, sick_leaves)
                await update_vacations(db, all_vacations)
                await update_vacations(db, special_users)
                # --- Создание / обновление виртуального филиала после background_tasks ---
                ids_aup = (1, 31, 2, 29, 28, 15, 21, 4, 25, 26, 27, 24, 3, 23, 16, 20, 61, 17, 18)
                await ensure_virtual_branch(db, ids_aup)
                logger.info("✅ Виртуальный филиал 'АУП' создан/обновлён")


async def ensure_virtual_branch(db, ids_aup: tuple[int], virtual_department_id: int = 99):
    """
    Создаёт виртуальный филиал 'АУП', если его нет, или обновляет его данные.
    virtual_department_id указывается в поле department_id, id остаётся автогенерируемым.
    """
    # Проверяем, есть ли филиал с таким department_id
    stmt = select(Branche).where(Branche.department_id == virtual_department_id)
    virtual_branch = (await db.execute(stmt)).scalar_one_or_none()

    if not virtual_branch:
        # Создаём виртуальный филиал
        virtual_branch = Branche(name="АУП", department_id=virtual_department_id)
        db.add(virtual_branch)
        await db.commit()
        await db.refresh(virtual_branch)

    # Получаем все филиалы и метрики
    branches = (await db.execute(select(Branche).order_by(Branche.id))).scalars().all()
    metrics = (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    branchdata_rows = (await db.execute(select(BranchData))).scalars().all()

    # --- Собираем последние значения по филиалам ---
    latest_data = {}
    for bd in branchdata_rows:
        key = (bd.branch_id, bd.metric_id)
        if key not in latest_data or bd.record_date > latest_data[key].record_date:
            latest_data[key] = bd

    # --- Агрегация метрик по филиалам AUP ---
    aggregated_metrics = defaultdict(float)
    latest_dates = {}
    metric_ids = {}
    relevant_branches = [b for b in branches if b.department_id in ids_aup]

    for branch in relevant_branches:
        for metric in metrics:
            bd = latest_data.get((branch.id, metric.id))
            if bd:
                aggregated_metrics[metric.name] += bd.value
                if (metric.name not in latest_dates) or (bd.record_date > latest_dates[metric.name]):
                    latest_dates[metric.name] = bd.record_date
                metric_ids[metric.name] = metric.id

    # --- Создаём или обновляем BranchData для виртуального филиала ---
    for metric_name, value in aggregated_metrics.items():
        metric_id = metric_ids[metric_name]
        record_date = latest_dates[metric_name] or date.today()

        stmt = select(BranchData).where(
            BranchData.branch_id == virtual_branch.id,
            BranchData.metric_id == metric_id,
            BranchData.record_date == record_date,
        )
        bd = (await db.execute(stmt)).scalar_one_or_none()
        if bd:
            bd.value = Decimal(str(value)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
            db.add(bd)
        else:
            db.add(
                BranchData(
                    branch_id=virtual_branch.id,
                    metric_id=metric_id,
                    record_date=record_date,
                    value=Decimal(str(value)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP),
                )
            )

    await db.commit()


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

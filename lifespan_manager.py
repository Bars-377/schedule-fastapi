import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta

import aiohttp
import aiomysql
from sqlmodel import SQLModel, select

from models import AsyncSessionLocal, BranchData, Branche, Metric, engine

# ==============================
# --- Конфигурация ---
# ==============================
with open("config.json", "r") as f:
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
            print(
                f"[{datetime.now():%Y-%m-%d %H:%M:%S}] ❌ Ошибка при работе \
                    с {name}: {e}. Повтор через {delay} сек (попытка {attempt})"
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
            print("[INFO] Подключение к MySQL...")
            # asyncio.wait_for ограничивает время await
            mysql_pool = await asyncio.wait_for(
                aiomysql.create_pool(**MYSQL_CONFIG), timeout=timeout
            )
            print("[OK] Успешное подключение к MySQL")
            return mysql_pool
        except asyncio.TimeoutError:
            print(f"[ERROR] Таймаут подключения к MySQL ({timeout} сек)")
        except Exception as e:
            print(
                f"[ERROR] [{datetime.now():%Y-%m-%d %H:%M:%S}] ❌ Ошибка подключения к MySQL: {e}. Повтор через 5 сек..."
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
    for dept in departments:
        name = dept.get("NAME", "").strip()
        department_id = int(dept.get("ID"))
        if "Отдел" not in name and "РЦТО" not in name:
            continue

        stmt = select(Branche).where(Branche.department_id == department_id)
        branch = (await db.execute(stmt)).scalar_one_or_none()

        if not branch:
            branch = Branche(name=name, department_id=department_id)
            db.add(branch)
            await db.commit()
            await db.refresh(branch)
            print(f"✅ Добавлен филиал: {name}")

        for metric in metrics:
            stmt_check = select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == today,
            )
            existing_record = (
                await db.execute(stmt_check)
            ).scalar_one_or_none()
            if not existing_record:
                db.add(
                    BranchData(
                        branch_id=branch.id,
                        metric_id=metric.id,
                        record_date=today,
                        value=0.0,
                    )
                )

    await db.commit()


import asyncio

async def process_vacations(session, users):
    all_vacations = {}
    sick_leaves = {}
    special_users = {}

    semaphore = asyncio.Semaphore(10)  # ограничиваем количество параллельных запросов

    async def process_user(user):
        employee_id = int(user["ID"])

        async with semaphore:
            absences = await fetch_absences_for_user_async(employee_id)
            user_info_result = await fetch_user_info(session, employee_id)

        if not user_info_result:
            return

        user_info = user_info_result[0]
        # print(user_info.get("UF_USR_1759203471311"))
        is_special_user = user_info.get("UF_USR_1759203471311") == '105'

        # Получаем department_id
        dept_id = None
        if "UF_DEPARTMENT" in user_info and user_info["UF_DEPARTMENT"]:
            dept_id_raw = user_info["UF_DEPARTMENT"][0]
            async with semaphore:
                dept_id = await fetch_department_info(session, dept_id_raw)
        if not dept_id:
            return

        # Спецпользователи для метрики 9
        if is_special_user:
            special_users.setdefault(dept_id, {}).setdefault(9, set()).add(employee_id)

        for absence in absences:
            absence_type = absence["ABSENCE_TYPE"]
            active_from = datetime.strptime(str(absence["ACTIVE_FROM"]), "%Y-%m-%d %H:%M:%S").date()
            active_to = datetime.strptime(str(absence["ACTIVE_TO"]), "%Y-%m-%d %H:%M:%S").date()
            if not (active_from <= today <= active_to):
                continue

            if absence_type == "больничный":
                sick_leaves.setdefault(dept_id, {}).setdefault(14, set()).add(employee_id)
            elif absence_type in (
                "отпуск ежегодный",
                "отпуск декретный",
                "отпуск без сохранения заработной платы",
            ):
                all_vacations.setdefault(dept_id, {}).setdefault(15, set()).add(employee_id)

    # Параллельная обработка всех пользователей
    await asyncio.gather(*(process_user(u) for u in users))

    # print(sick_leaves)
    # print('-------------------------')
    # print(all_vacations)
    # print('-------------------------')
    # print(special_users)
    # print('-------------------------')

    return sick_leaves, all_vacations, special_users


async def update_vacations(db, departments_employees):
    for dept_id, metrics_dict in departments_employees.items():
        stmt_branch = select(Branche).where(
            Branche.department_id == int(dept_id)
        )
        branch = (await db.execute(stmt_branch)).scalar_one_or_none()
        if not branch:
            print(f"[WARNING] Филиал для department_id={dept_id} не найден")
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
                print(
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
                print(
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
        print(
            f"[INFO] Следующее обновление через {wait_seconds / 3600:.2f} ч."
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


# ==============================
# --- Lifespan FastAPI ---
# ==============================
@asynccontextmanager
async def lifespan(app):
    print(f"Сервер запущен в {datetime.now():%Y-%m-%d %H:%M:%S}")

    # Создаём таблицы (await, но это быстро)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    # --- Фоновая задача для обновлений ---
    async def background_tasks():
        # Инициализация MySQL пула
        try:
            await init_mysql_pool()
        except Exception as e:
            print(f"[ERROR] Не удалось инициализировать MySQL: {e}")

        while True:
            try:
                # Здесь внутри schedule_update_loop уже свой retry_forever
                await schedule_update_loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[ERROR] Фоновое обновление завершилось с ошибкой: {e}")
                await asyncio.sleep(30)  # ждём перед повтором

    # Запускаем таск **без await**, чтобы FastAPI стартовал сразу
    task = asyncio.create_task(background_tasks())

    try:
        yield
    finally:
        # При shutdown отменяем таск
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        if mysql_pool:
            mysql_pool.close()
            await mysql_pool.wait_closed()
        await engine.dispose()

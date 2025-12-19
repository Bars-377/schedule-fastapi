"""
Рефакторинг

Основные улучшения:
- Инкапсуляция состояния в классе LifespanManager для уменьшения глобального состояния.
- Чёткие аннотации типов.
- Унификация логирования и сообщений.
- Явные проверки и обработка исключений в сетевых/БД-вызовах.
- Улучшенные docstring'и и читаемость.
- Сохранена совместимость API функций (асинхронные функции с теми же семантиками).
"""

from __future__ import annotations

import asyncio
import json
import logging
import smtplib
from collections.abc import Iterable
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import aiohttp
import aiomysql
from models import AsyncSessionLocal, BranchData, Branche, Metric, engine
from recalc import recalc
from sqlmodel import SQLModel, select

# ==============================
# --- Логирование (глобально) ---
# ==============================
logger = logging.getLogger("branch_update")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# ==============================
# --- Конфигурация (загружается один раз) ---
# ==============================
with open("config.json", encoding="utf-8") as f:
    CONFIG: dict[str, Any] = json.load(f)

PREVIOUS_METRIC_NAMES: set[str] = {n.lower() for n in CONFIG.get("previous_metrics", [])}

BITRIX_BASE_URL = CONFIG.get("bitrix_base_url", "https://bitrix.mfc.tomsk.ru/rest/533/dfk26tp3grjqm2b4")
BITRIX_USER_LIST_URL = f"{BITRIX_BASE_URL}/user.get.json?ADMIN_MODE=True&SORT=ID&ORDER=ASC&start={{start}}"
BITRIX_USER_INFO_URL = f"{BITRIX_BASE_URL}/user.get.json?id={{user_id}}"
BITRIX_DEPARTMENT_URL = f"{BITRIX_BASE_URL}/department.get.json?ID={{dept_id}}"

UPDATE_HOUR_ONE = CONFIG.get("update_hour_one", 10)
UPDATE_MINUTE_ONE = CONFIG.get("update_minute_one", 0)
UPDATE_HOUR_TWO = CONFIG.get("update_hour_two", 16)
UPDATE_MINUTE_TWO = CONFIG.get("update_minute_two", 0)

MYSQL_CONFIG = {
    "host": CONFIG["mysql"]["host"],
    "user": CONFIG["mysql"]["user"],
    "password": CONFIG["mysql"]["password"],
    "db": CONFIG["mysql"]["database"],
    "charset": CONFIG["mysql"]["charset"],
}

# Bitrix client defaults
BITRIX_SEMAPHORE_DEFAULT = 10
BITRIX_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)

# ==============================
# --- Вспомогательные типы ---
# ==============================
MetricMap = dict[str, int]
DeptMetricMap = dict[int, dict[int, set[int]]]
StaffingMap = dict[int, dict[int, set[float]]]


# ==============================
# --- Retry helper (реиспользуемый) ---
# ==============================
async def retry_forever(coro_func, *args, delay: int = 5, name: str = "unknown", **kwargs):
    """
    Повторяет вызов асинхронной функции пока не выполнится успешно.
    Позволяет логировать и ждать между попытками.
    """
    attempt = 1
    while True:
        try:
            return await coro_func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"[{name}] Ошибка (попытка {attempt}): {exc}", exc_info=True)
            await asyncio.sleep(delay)
            attempt += 1


# ==============================
# --- Основной класс менеджера ---
# ==============================
class LifespanManager:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.previous_metric_names: set[str] = {n.lower() for n in config.get("previous_metrics", [])}
        self.mysql_pool: aiomysql.Pool | None = None
        self.mysql_connected: bool = False
        self.bitrix_semaphore = asyncio.Semaphore(config.get("bitrix_semaphore", BITRIX_SEMAPHORE_DEFAULT))
        self.email_queue: asyncio.Queue = asyncio.Queue()
        self.email_semaphore = asyncio.Semaphore(1)
        self._email_workers: list[asyncio.Task] = []
        self._background_task: asyncio.Task | None = None

    # ------------------------------
    # MySQL pool
    # ------------------------------
    async def init_mysql_pool(self, timeout: int = 10) -> aiomysql.Pool:
        if self.mysql_connected and self.mysql_pool is not None:
            return self.mysql_pool

        while True:
            try:
                logger.info("Подключение к MySQL...")
                self.mysql_pool = await asyncio.wait_for(
                    aiomysql.create_pool(minsize=1, maxsize=10, **MYSQL_CONFIG),
                    timeout=timeout
                )
                self.mysql_connected = True
                logger.info("✅ Успешное подключение к MySQL")
                return self.mysql_pool
            except TimeoutError:
                logger.error(f"⏱ Таймаут подключения к MySQL ({timeout} сек)")
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к MySQL: {e}", exc_info=True)
            await asyncio.sleep(5)

    async def close_mysql_pool(self):
        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None
            self.mysql_connected = False
            logger.info("MySQL пул закрыт")

    # ------------------------------
    # Bitrix helpers
    # ------------------------------
    async def fetch_json(self, session: aiohttp.ClientSession, url: str) -> Any:
        async def _fetch():
            async with self.bitrix_semaphore:
                async with session.get(url, timeout=BITRIX_TIMEOUT) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    return data.get("result", [])
        return await retry_forever(_fetch, name=f"Bitrix {url}")

    async def fetch_departments_from_bitrix(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        logger.info("Загрузка списка отделов из Bitrix...")
        return await self.fetch_json(session, f"{BITRIX_BASE_URL}/department.get.json")

    async def fetch_users_from_bitrix(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        logger.info("Загрузка списка пользователей из Bitrix...")
        all_users: list[dict[str, Any]] = []
        start = 0
        page_size = 50
        while True:
            result = await self.fetch_json(session, BITRIX_USER_LIST_URL.format(start=start))
            if not result:
                break
            all_users.extend(result)
            start += page_size
        logger.info(f"Всего загружено пользователей: {len(all_users)}")
        return all_users

    async def fetch_user_info(self, session: aiohttp.ClientSession, employee_id: int) -> list[dict[str, Any]]:
        return await self.fetch_json(session, BITRIX_USER_INFO_URL.format(user_id=employee_id))

    async def fetch_department_info(self, session: aiohttp.ClientSession, dept_id: int) -> tuple[str | None, str | None]:
        result = await self.fetch_json(session, BITRIX_DEPARTMENT_URL.format(dept_id=dept_id))
        if not result:
            return None, None
        row = result[0]
        return (row.get("ID", "").strip(), row.get("NAME", "").strip())

    # ------------------------------
    # Работы с метриками и BranchData
    # ------------------------------
    async def update_branchdata_value(self, branchdata: BranchData, value: float, log_prefix: str = ""):
        old_value = branchdata.value
        branchdata.value = Decimal(value)
        logger.info(
            f"{log_prefix} Обновлено значение BranchData (branch_id={branchdata.branch_id}, metric_id={branchdata.metric_id}): {old_value} -> {value}"
        )

    async def update_branches(self, db, departments: Iterable[dict[str, Any]], metrics: Iterable[Metric], today: date):
        for dept in departments:
            name = dept.get("NAME", "").strip()
            department_id = int(dept.get("ID"))
            stmt = select(Branche).where(Branche.department_id == department_id)
            branch = (await db.execute(stmt)).scalar_one_or_none()
            if not branch:
                branch = Branche(name=name, department_id=department_id)
                db.add(branch)
                await db.flush()
                logger.info(f"✅ Добавлен новый филиал: {name} (ID={department_id})")

            for metric in metrics:
                stmt_check = select(BranchData).where(
                    BranchData.branch_id == branch.id,
                    BranchData.metric_id == metric.id,
                    BranchData.record_date == today,
                )
                existing_record = (await db.execute(stmt_check)).scalar_one_or_none()

                if existing_record:
                    if metric.name.lower() in self.previous_metric_names:
                        stmt_prev = select(BranchData).where(
                            BranchData.branch_id == branch.id,
                            BranchData.metric_id == metric.id,
                            BranchData.record_date < today,
                        ).order_by(BranchData.record_date.desc()).limit(1)
                        prev_record = (await db.execute(stmt_prev)).scalar_one_or_none()
                        existing_record.value = prev_record.value if prev_record else Decimal("0.00")
                    else:
                        existing_record.value = Decimal("0.00")
                    db.add(existing_record)
                else:
                    if metric.name.lower() in self.previous_metric_names:
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

            await db.flush()
            await recalc(db, today, branch.id)

        await db.commit()

    async def _load_metrics_from_db(self) -> MetricMap:
        async with AsyncSessionLocal() as db:
            metrics = (await db.execute(select(Metric))).scalars().all()
        return {m.name.lower(): m.id for m in metrics}

    def _check_missing_metrics(self, metric_map: MetricMap, metric_names: dict[str, str]):
        missing = [name for name in metric_names.values() if name and not metric_map.get(name)]
        if missing:
            logger.warning(f"Не найдены метрики в таблице Metric: {', '.join(missing)}")

    # ------------------------------
    # Email helpers (асинхронная очередь)
    # ------------------------------
    async def _send_email_task(self, message: EmailMessage, smtp_server: str, smtp_port: int, max_attempts: int, retry_delay: int) -> bool:
        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.to_thread(self._send_email_sync, message, smtp_server, smtp_port)
                logger.info("✅ Письмо успешно отправлено.")
                await asyncio.sleep(retry_delay)
                return True
            except smtplib.SMTPException as e:
                logger.error(f"Попытка {attempt} ❌ Ошибка при отправке письма: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(retry_delay)
        logger.error("❌ Не удалось отправить письмо после всех попыток.")
        return False

    @staticmethod
    def _send_email_sync(message: EmailMessage, smtp_server: str, smtp_port: int):
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.send_message(message)

    async def send_email_async(
        self,
        subject: str,
        body: str,
        to: list[str],
        attachments: list[Path] | None = None,
        smtp_server: str = "smtp.mfc.tomsk.ru",
        smtp_port: int = 25,
        sender_email: str = "oprgp.toma@mfc.tomsk.ru",
        max_attempts: int = 3,
        retry_delay: int = 5,
    ) -> bool:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = ", ".join(to)
        message.set_content(body)

        if attachments:
            for path in attachments:
                path = Path(path)
                if path.exists():
                    with path.open("rb") as f:
                        message.add_attachment(
                            f.read(),
                            maintype="application",
                            subtype="octet-stream",
                            filename=path.name
                        )
                else:
                    logger.warning(f"Файл {path} не найден, пропускаем.")

        return await self._send_email_task(message, smtp_server, smtp_port, max_attempts, retry_delay)

    async def _email_worker(self):
        while True:
            email_task = await self.email_queue.get()
            try:
                async with self.email_semaphore:
                    await self.send_email_async(**email_task)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка при отправке письма: {e}", exc_info=True)
            finally:
                self.email_queue.task_done()
                await asyncio.sleep(2)

    async def start_email_workers(self, n_workers: int = 2):
        for _ in range(n_workers):
            t = asyncio.create_task(self._email_worker())
            self._email_workers.append(t)

    async def stop_email_workers(self):
        for t in self._email_workers:
            t.cancel()
        for t in self._email_workers:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._email_workers.clear()

    async def queue_email(
        self,
        subject: str,
        body: str,
        to: list[str],
        attachments: list[Path] | None = None,
        smtp_server: str = "smtp.mfc.tomsk.ru",
        smtp_port: int = 25,
        sender_email: str = "oprgp.toma@mfc.tomsk.ru",
        max_attempts: int = 3,
        retry_delay: int = 5,
    ):
        await self.email_queue.put({
            "subject": subject,
            "body": body,
            "to": to,
            "attachments": attachments,
            "smtp_server": smtp_server,
            "smtp_port": smtp_port,
            "sender_email": sender_email,
            "max_attempts": max_attempts,
            "retry_delay": retry_delay
        })

    # ------------------------------
    # Обработка пользователей / отпусков / больничных
    # ------------------------------
    async def _process_single_user(
        self,
        user: dict[str, Any],
        session: aiohttp.ClientSession,
        metric_ids: dict[str, int | None],
        sick_leaves: DeptMetricMap,
        all_vacations: DeptMetricMap,
        special_users: DeptMetricMap,
        semaphore: asyncio.Semaphore,
        today: date,
    ):
        employee_id = int(user["ID"])
        async with semaphore:
            absences = await self.fetch_absences_for_user_async(employee_id)
            user_info_result = await self.fetch_user_info(session, employee_id)

        if not user_info_result:
            return

        user_info = user_info_result[0]
        is_special_user = user_info.get("UF_USR_1759203471311") == "105"

        dept_id: int | None = None
        dept_name: str | None = None
        if "UF_DEPARTMENT" in user_info and user_info["UF_DEPARTMENT"]:
            dept_id_raw = user_info["UF_DEPARTMENT"][0]
            async with semaphore:
                dept_id_str, dept_name = await self.fetch_department_info(session, dept_id_raw)
                try:
                    dept_id = int(dept_id_str) if dept_id_str else None
                except (ValueError, TypeError):
                    dept_id = None

        if not dept_id:
            return

        if is_special_user and metric_ids.get("special"):
            special_users.setdefault(dept_id, {}).setdefault(metric_ids["special"], set()).add(employee_id)

        now = datetime.now()

        SEND_AT_10 = time(10, 0)
        SEND_AT_16 = time(16, 0)
        SEND_WINDOW = timedelta(minutes=5)  # защита от дублей

        vacations = {v.lower() for v in (self.config.get("vacations") or [])}
        sick_leave = {s.lower() for s in (self.config.get("sick_leave") or [])}

        for absence in absences:
            absence_type = (absence.get("ABSENCE_TYPE") or "").lower()
            try:
                active_from_dt = datetime.strptime(str(absence["ACTIVE_FROM"]), "%Y-%m-%d %H:%M:%S")
                active_to_dt = datetime.strptime(str(absence["ACTIVE_TO"]), "%Y-%m-%d %H:%M:%S")
                active_from = active_from_dt.date()
                active_to = active_to_dt.date()
            except Exception:
                # Если формат даты отличается — пропускаем запись
                logger.warning(f"Неверный формат дат отсутствия для сотрудника {employee_id}: {absence}")
                continue

            if (active_from <= today - timedelta(days=1) <= active_to):
                # больничные
                if absence_type in sick_leave and metric_ids.get("sick"):
                    sick_leaves.setdefault(dept_id, {}).setdefault(metric_ids["sick"], set()).add(employee_id)

                # отпуск
                elif absence_type in vacations and metric_ids.get("vacation"):
                    all_vacations.setdefault(dept_id, {}).setdefault(metric_ids["vacation"], set()).add(employee_id)
                    continue  # ⛔ больничные — без уведомлений

                # прочие отсутствия
                elif metric_ids.get("absence"):
                    all_vacations.setdefault(dept_id, {}).setdefault(metric_ids["absence"], set()).add(employee_id)

            if absence_type in vacations:
                continue  # ⛔ гарантированно исключаем отпуск

            # ---------------- уведомления ----------------

            date_create_str = absence.get("DATE_CREATE")
            try:
                date_create_dt = datetime.strptime(str(date_create_str), "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            create_time = date_create_dt.time()
            create_date = date_create_dt.date()

            # ---- вычисляем единственный момент отправки ----

            if create_time >= SEND_AT_16:
                # с 16:00 до 23:59 → 10:00 следующего дня
                send_dt = datetime.combine(create_date + timedelta(days=1), SEND_AT_10)

            elif create_time < SEND_AT_10:
                # с 00:00 до 09:59 → 10:00 этого дня
                send_dt = datetime.combine(create_date, SEND_AT_10)

            else:
                # с 10:00 до 15:59 → 16:00 этого дня
                send_dt = datetime.combine(create_date, SEND_AT_16)

            # ---- защита от дублей ----

            if not (send_dt <= now < send_dt + SEND_WINDOW):
                continue

            asyncio.create_task(self.queue_email(
                subject=(
                    f"{absence.get('ABSENCE_NAME', '').capitalize()} "
                    f"{user_info.get('LAST_NAME', '')} "
                    f"{user_info.get('NAME', '')} "
                    f"{user_info.get('SECOND_NAME', '')} "
                    f"в {dept_name or ''}"
                ),
                body=(
                    f"{user_info.get('LAST_NAME', '')} "
                    f"{user_info.get('NAME', '')} "
                    f"{user_info.get('SECOND_NAME', '')} — "
                    f"{absence.get('ABSENCE_NAME', '')} "
                    f"с {active_from} по {active_to} "
                    f"в {dept_name or ''}. "
                    f"Дата внесения {absence.get('DATE_CREATE', '')}"
                ),
                to=self.config.get("notify_emails", ["neverov@mfc.tomsk.ru"]),
            ))

    async def fetch_absences_for_user_async(self, employee_id: int):
        pool = await self.init_mysql_pool()
        query = """
            SELECT
                e.ID AS ELEMENT_ID,
                e.NAME AS ABSENCE_NAME,
                e.DATE_CREATE,
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
                self.mysql_connected = False
                if self.mysql_pool:
                    self.mysql_pool.close()
                    await self.mysql_pool.wait_closed()
                    self.mysql_pool = None
                raise

        return await retry_forever(_fetch, name=f"MySQL fetch_absences ID={employee_id}")

    async def process_vacations(self, session: aiohttp.ClientSession, users: list[dict[str, Any]], today: date) -> tuple[DeptMetricMap, DeptMetricMap, DeptMetricMap]:
        metric_config = self.config.get("metrics", {})
        metric_names = {
            "special": metric_config.get("special", "").lower(),
            "sick": metric_config.get("sick", "").lower(),
            "vacation": metric_config.get("vacation", "").lower(),
            "absence": metric_config.get("absence", "").lower(),
        }

        metric_map = await self._load_metrics_from_db()
        self._check_missing_metrics(metric_map, metric_names)

        metric_ids = {
            "special": metric_map.get(metric_names["special"]),
            "sick": metric_map.get(metric_names["sick"]),
            "vacation": metric_map.get(metric_names["vacation"]),
            "absence": metric_map.get(metric_names["absence"]),
        }

        all_vacations: DeptMetricMap = {}
        sick_leaves: DeptMetricMap = {}
        special_users: DeptMetricMap = {}
        semaphore = asyncio.Semaphore(10)

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
                if metric_ids.get("special"):
                    special_users.setdefault(dept_id, {})[metric_ids["special"]] = set()

        tasks = [
            self._process_single_user(user, session, metric_ids, sick_leaves, all_vacations, special_users, semaphore, today)
            for user in users
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        return sick_leaves, all_vacations, special_users

    async def update_vacations(self, db, departments_employees: DeptMetricMap, today: date, condition: bool = False):
        for dept_id, metrics_dict in departments_employees.items():
            stmt_branch = select(Branche).where(Branche.department_id == int(dept_id))
            branch = (await db.execute(stmt_branch)).scalar_one_or_none()
            if not branch:
                logger.warning(f"Филиал для department_id={dept_id} не найден")
                continue
            logger.info(f"Обновление метрик филиала {branch.name} (department_id={dept_id})")

            for metric_id, employees_set in metrics_dict.items():
                if condition:
                    # condition означает: взять первый элемент вместо длины (используется для staffing_analysis)
                    value = next(iter(employees_set), 0) if employees_set else 0
                else:
                    value = len(employees_set) if employees_set else 0
                stmt_data = select(BranchData).where(
                    BranchData.branch_id == branch.id,
                    BranchData.metric_id == metric_id,
                    BranchData.record_date == today,
                )
                bd = (await db.execute(stmt_data)).scalar_one_or_none()
                if bd:
                    await self.update_branchdata_value(bd, value, log_prefix=f"{branch.name}")
                else:
                    bd = BranchData(branch_id=branch.id, metric_id=metric_id, record_date=today, value=value)
                    db.add(bd)
                    logger.info(f"➕ Добавлена новая запись BranchData: {branch.name}, metric_id={metric_id} -> {value}")
        await db.commit()

    # ------------------------------
    # Виртуальный филиал "АУП"
    # ------------------------------
    async def get_or_create_virtual_branch(self, db, virtual_department_id: int = 99, name: str = "АУП") -> Branche:
        stmt = select(Branche).where(Branche.department_id == virtual_department_id)
        branch = (await db.execute(stmt)).scalar_one_or_none()
        if not branch:
            branch = Branche(name=name, department_id=virtual_department_id)
            db.add(branch)
            await db.commit()
            await db.refresh(branch)
        return branch

    async def ensure_virtual_branch(self, db, ids_aup: Iterable[int], today: date, virtual_department_id: int = 99, name: str = "АУП"):
        virtual_branch = await self.get_or_create_virtual_branch(db, virtual_department_id, name)

        metrics = (await db.execute(select(Metric))).scalars().all()

        stmt_branches = select(Branche).where(Branche.department_id.in_(ids_aup))
        branches = (await db.execute(stmt_branches)).scalars().all()
        branch_ids = [b.id for b in branches]

        stmt_branchdata = select(BranchData).where(
            BranchData.branch_id.in_(branch_ids),
            BranchData.record_date == today
        )
        branchdata_list = (await db.execute(stmt_branchdata)).scalars().all()

        branch_data_map: dict[int, dict[int, Decimal]] = {}
        for bd in branchdata_list:
            branch_data_map.setdefault(bd.branch_id, {})[bd.metric_id] = bd.value

        prev_metrics_ids = [m.id for m in metrics if m.name.lower() in self.previous_metric_names]
        stmt_prev_bd = select(BranchData).where(
            BranchData.branch_id == virtual_branch.id,
            BranchData.metric_id.in_(prev_metrics_ids),
            BranchData.record_date < today
        ).order_by(BranchData.metric_id, BranchData.record_date.desc())
        prev_branchdata_list = (await db.execute(stmt_prev_bd)).scalars().all()

        last_prev_value_map: dict[int, Decimal] = {}
        for bd in prev_branchdata_list:
            if bd.metric_id not in last_prev_value_map:
                last_prev_value_map[bd.metric_id] = bd.value

        for metric in metrics:
            stmt_check = select(BranchData).where(
                BranchData.branch_id == virtual_branch.id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == today
            )
            bd = (await db.execute(stmt_check)).scalar_one_or_none()

            if metric.name.lower() in self.previous_metric_names:
                if bd and bd.value != Decimal("0.00"):
                    value = bd.value
                else:
                    prev_value = last_prev_value_map.get(metric.id, Decimal("0.00"))
                    value = prev_value if prev_value != Decimal("0.00") else Decimal("0.00")
            else:
                value = sum(
                    branch_data_map.get(branch_id, {}).get(metric.id, Decimal("0.00"))
                    for branch_id in branch_ids
                )

            if not bd:
                bd = BranchData(branch_id=virtual_branch.id, metric_id=metric.id, record_date=today, value=value)
                db.add(bd)
            else:
                bd.value = value

        await db.flush()
        await recalc(db, today, virtual_branch.id)
        await db.commit()

    # ------------------------------
    # staffing_analysis (MySQL отдельной БД)
    # ------------------------------
    async def staffing_analysis(self, db, today: date) -> StaffingMap:
        pool: aiomysql.Pool | None = None
        try:
            pool = await aiomysql.create_pool(
                host=self.config["mysql_mdtomskbot"]["host"],
                user=self.config["mysql_mdtomskbot"]["user"],
                password=self.config["mysql_mdtomskbot"]["password"],
                db=self.config["mysql_mdtomskbot"]["database"],
                charset=self.config["mysql_mdtomskbot"]["charset"],
                minsize=1,
                maxsize=5,
            )

            yesterday = today - timedelta(days=1)

            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("""
                        SELECT id_one_c, dep_id, planned, free, date, vacations, hospital
                        FROM staffing_analysis
                        WHERE date = %s
                    """, (yesterday,))
                    rows = await cur.fetchall()
                    logger.info("Запрошены данные staffing_analysis из MySQL")

            id_to_dep = {row["id_one_c"]: int(row["dep_id"]) for row in rows if row["dep_id"] is not None}

            result_metrics = await db.execute(select(Metric))
            metrics_list = result_metrics.scalars().all()
            metric_name_to_id = {m.name: m.id for m in metrics_list}

            temp_result: dict[int, dict[int, set[float]]] = {}
            for row in rows:
                id_one_c = row["id_one_c"]
                dep_id = id_to_dep.get(id_one_c)
                if dep_id is None:
                    continue

                planned_value = float(row.get("planned") or 0)
                free_value = float(row.get("free") or 0)
                hospital = float(row.get("hospital") or 0)
                vacations = float(row.get("vacations") or 0)

                temp_result.setdefault(dep_id, {})
                temp_result[dep_id].setdefault(metric_name_to_id.get("Штатная численность (1С)"), set()).add(planned_value + free_value)
                temp_result[dep_id].setdefault(metric_name_to_id.get("Свободные ставки (1С)"), set()).add(free_value)
                temp_result[dep_id].setdefault(metric_name_to_id.get("Б/л (1С)"), set()).add(hospital)
                temp_result[dep_id].setdefault(metric_name_to_id.get("Отпуск (1С)"), set()).add(vacations)

            ids_aup = set(self.config.get("ids_aup", []))
            ids_aup.add(99)
            sum_planned_free = sum(sum(temp_result.get(aup_id, {}).get(metric_name_to_id.get("Штатная численность (1С)"), set())) for aup_id in ids_aup)
            sum_free = sum(sum(temp_result.get(aup_id, {}).get(metric_name_to_id.get("Свободные ставки (1С)"), set())) for aup_id in ids_aup)
            sum_hospital = sum(sum(temp_result.get(aup_id, {}).get(metric_name_to_id.get("Б/л (1С)"), set())) for aup_id in ids_aup)
            sum_vacations = sum(sum(temp_result.get(aup_id, {}).get(metric_name_to_id.get("Отпуск (1С)"), set())) for aup_id in ids_aup)

            temp_result[99] = {
                metric_name_to_id.get("Штатная численность (1С)"): {sum_planned_free},
                metric_name_to_id.get("Свободные ставки (1С)"): {sum_free},
                metric_name_to_id.get("Б/л (1С)"): {sum_hospital},
                metric_name_to_id.get("Отпуск (1С)"): {sum_vacations},
            }

            return temp_result

        finally:
            if pool:
                pool.close()
                await pool.wait_closed()

    # ------------------------------
    # Главный процесс верarbeitung (перевычисление)
    # ------------------------------
    async def verarbeitung(self, session: aiohttp.ClientSession, users: list[dict[str, Any]], departments: list[dict[str, Any]], today: date):
        async with AsyncSessionLocal() as db:
            metrics = (await db.execute(select(Metric))).scalars().all()
            await self.update_branches(db, departments, metrics, today)

        sick_leaves, all_vacations, special_users = await self.process_vacations(session, users, today)

        async with AsyncSessionLocal() as db:
            staffing = await self.staffing_analysis(db, today)
            await self.update_vacations(db, staffing, today, condition=True)

            await self.update_vacations(db, sick_leaves, today)
            await self.update_vacations(db, all_vacations, today)
            await self.update_vacations(db, special_users, today)

        async with AsyncSessionLocal() as db:
            ids_aup = set(self.config.get("ids_aup", []))
            await self.ensure_virtual_branch(db, ids_aup, today)
            logger.info("✅ Виртуальный филиал 'АУП' создан/обновлён")

        async with AsyncSessionLocal() as db:
            await recalc(db, today)
            await db.commit()

    # ------------------------------
    # Цикл планировщика
    # ------------------------------
    async def schedule_update_loop(self):
        await asyncio.sleep(3)
        while True:
            now = datetime.now()

            # задаем два времени обновления на сегодня
            target_times = [
                now.replace(hour=UPDATE_HOUR_ONE, minute=UPDATE_MINUTE_ONE, second=0, microsecond=0),
                now.replace(hour=UPDATE_HOUR_TWO, minute=UPDATE_MINUTE_TWO, second=0, microsecond=0)
            ]

            # выбираем ближайшее будущее время
            future_times = [t for t in target_times if t > now]
            if not future_times:
                # если оба времени уже прошли, ставим на завтра
                target_times = [
                    (target_times[0] + timedelta(days=1)),
                    (target_times[1] + timedelta(days=1))
                ]
                future_times = target_times

            next_update = min(future_times)
            wait_seconds = (next_update - now).total_seconds()
            logger.info(f"Следующее обновление через {wait_seconds / 3600:.2f} ч.")
            await asyncio.sleep(wait_seconds)

            async with aiohttp.ClientSession(timeout=BITRIX_TIMEOUT) as session:
                departments = await self.fetch_departments_from_bitrix(session)
                users = await self.fetch_users_from_bitrix(session)
                days = [date.today()]  # можно переопределить при вызове
                # days = [date(2025, 11, 21), date(2025, 12, 4)]

                if len(days) == 1:
                    await self.verarbeitung(session, users, departments, days[0])
                else:
                    start = min(days)
                    end = max(days)
                    today = start
                    while today <= end:
                        await self.verarbeitung(session, users, departments, today)
                        today += timedelta(days=1)

    # ------------------------------
    # Lifespan helpers (стартап/шутдаун)
    # ------------------------------
    async def start_background_tasks(self):
        # инициализация БД схем
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

        try:
            await self.init_mysql_pool()
        except Exception as e:
            logger.error(f"Не удалось инициализировать MySQL: {e}", exc_info=True)
            return

        await self.start_email_workers(n_workers=self.config.get("email_workers", 2))
        self._background_task = asyncio.create_task(self.schedule_update_loop())

    async def stop_background_tasks(self):
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None

        await self.stop_email_workers()
        await self.close_mysql_pool()
        await engine.dispose()


# ==============================
# --- FastAPI lifespan (asynccontextmanager) ---
# ==============================
@asynccontextmanager
async def lifespan(app):
    manager = LifespanManager(CONFIG)
    logger.info(f"Сервер запущен в {datetime.now():%Y-%m-%d %H:%M:%S}")

    if CONFIG.get("enable_background_task", True):
        logger.info("✅ Фоновая задача включена")
        await manager.start_background_tasks()
    else:
        logger.info("⚠️ Фоновая задача отключена")
        await manager.start_email_workers(n_workers=CONFIG.get("email_workers", 2))

    try:
        yield
    finally:
        logger.info("Останавливаем фоновые задачи...")
        await manager.stop_background_tasks()

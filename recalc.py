import json
from datetime import date
from decimal import Decimal

from models import BranchData, Branche, Metric
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func, or_, select

with open("config.json", encoding="utf-8") as f:
    config = json.load(f)

# ---------------------------------------------------------
# ------------------ Вспомогательные функции --------------
# ---------------------------------------------------------

def _quarter_start(dt: date) -> date:
    """Возвращает дату начала квартала по дате dt."""
    quarter_month = ((dt.month - 1) // 3) * 3 + 1
    return date(dt.year, quarter_month, 1)


def _calc_metric3(values: dict[str, Decimal]) -> Decimal:
    """Метрика 3: value0 - value4 - value5 - value6"""
    result = (
        values[config.get("metrics", []).get("state", [])]
        - values[config.get("metrics", []).get("sick", [])]
        - values[config.get("metrics", []).get("vacation", [])]
        - values[config.get("metrics", []).get("free", [])]
    )
    if result < 0:
        raise ValueError("Отрицательное вычисление")
    return Decimal(result)


def _calc_metric4(values: dict[str, Decimal]) -> Decimal:
    """Метрика 4: (value2 * 100) / value0"""
    staff = values[config.get("metrics", []).get("state", [])]
    if staff == 0:
        raise ValueError("Делить на 0 нельзя")
    # print('-------------------')
    # print(values[config.get("metrics", []).get("fact", [])])
    # print(staff)
    # print(Decimal(values[config.get("metrics", []).get("fact", [])] * 100 / staff))
    return Decimal(values[config.get("metrics", []).get("fact", [])] * 100 / staff)


async def _calc_metric8(branch_id: int, db: AsyncSession, cache_metric: Decimal, metric_id_to_name) -> Decimal:
    """Метрика 8 — среднее значение метрики 3 с корректировкой на прогресс квартала."""
    dates_query = await db.execute(
        select(func.min(BranchData.record_date), func.max(BranchData.record_date))
        .where(BranchData.branch_id == branch_id)
    )
    min_date, max_date = dates_query.one()

    if not max_date or cache_metric == 0:
        return Decimal(0)

    start = _quarter_start(max_date)
    if min_date and min_date > start:
        start = min_date

    days = (max_date - start).days + 1
    if days <= 0:
        return Decimal(0)

    sum_query = await db.execute(
        select(func.sum(BranchData.value)).where(
            BranchData.branch_id == branch_id,
            BranchData.metric_id == metric_id_to_name.get("Фактическое число работающих (ед.)"),
            BranchData.record_date.between(start, max_date),
        )
    )
    sum_metric3 = Decimal(sum_query.scalar() or 0)

    # print(sum_metric3)
    # print(Decimal(days))
    # print(cache_metric)
    # print('fddsf')
    # print(sum_metric3 / Decimal(days) * Decimal(100) / cache_metric)

    return sum_metric3 / Decimal(days) * Decimal(100) / cache_metric


# ---------------------------------------------------------
# -------------------- Логика обновления ------------------
# ---------------------------------------------------------

async def _apply_updates(branchdata_list, new_values, message, db: AsyncSession):
    """Применение обновлённых значений в базу."""
    if not message:
        for bd in branchdata_list:
            bd.value = new_values.get(bd.id, bd.value)
            db.add(bd)
    else:
        # Обнуляем определённые индексы при ошибках
        for i, bd in enumerate(branchdata_list):
            if i in (0, 2, 3, 7):
                bd.value = Decimal("0")
                db.add(bd)


async def _calculate_new_values(
    branchdata_id: int,
    branchdata_list: list[BranchData],
    db: AsyncSession,
    metrics_names: dict[int, str]
):

    # Имена метрик из БД
    result = await db.execute(select(Metric))
    metrics_list = result.scalars().all()  # <-- список объектов Metric

    metric_id_to_name = {m.name: m.id for m in metrics_list}

    # print(metric_id_to_name)

    """Вычисление новых значений метрик."""
    new_values = {}
    message = None

    # Значение для кэша 8-й метрики
    cache_metric = branchdata_list[0].value

    # Словарь {название_метрики: значение}
    original_values = {
        metrics_names[bd.metric_id]: Decimal(bd.value)
        for bd in branchdata_list
    }

    for bd in branchdata_list:
        if bd.id == branchdata_id:
            new_values[bd.id] = bd.value
            continue

        try:
            if bd.metric_id == metric_id_to_name.get("Фактическое число работающих (ед.)"):
                new_values[bd.id] = _calc_metric3(original_values)

            elif bd.metric_id == metric_id_to_name.get("Фактическое число работающих от шт. численности (%)"):
                new_values[bd.id] = _calc_metric4(original_values)

            elif bd.metric_id == metric_id_to_name.get("Квартал Боевая численность"):
                new_values[bd.id] = await _calc_metric8(
                    bd.branch_id, db, Decimal(cache_metric), metric_id_to_name
                )

            else:
                new_values[bd.id] = bd.value

        except ValueError as e:
            message = str(e)
            break

    return message, new_values


# ---------------------------------------------------------
# ---------------------- Пересчёт --------------------------
# ---------------------------------------------------------

async def _load_aup_ids():
    """Получение id подразделений АУП из конфига."""
    return set(map(int, config.get("ids_aup", [])))


async def _load_metrics_names(db: AsyncSession, branchdata_list: list[BranchData]) -> dict[int, str]:
    """Создаёт словарь {id метрики: имя метрики}."""
    metric_ids = {bd.metric_id for bd in branchdata_list}

    result = await db.execute(
        select(Metric.id, Metric.name).where(Metric.id.in_(metric_ids))
    )
    return {row[0]: row[1] for row in result.all()}


async def recalc(db: AsyncSession, target_date: date, branch_id: int | None = None):
    """Пересчёт всех метрик филиала на выбранную дату."""
    if branch_id:
        branch_ids = [branch_id]
    else:
        ids_aup = await _load_aup_ids()
        result = await db.execute(
            select(Branche.id).where(
                or_(Branche.department_id.is_(None),
                    ~Branche.department_id.in_(ids_aup))
            ).distinct()
        )
        branch_ids = [row[0] for row in result.all()]

    messages = {}

    for bid in branch_ids:
        query = await db.execute(
            select(BranchData)
            .where(
                BranchData.branch_id == bid,
                BranchData.record_date == target_date,
            )
            .order_by(BranchData.metric_id)
        )
        branchdata_list = query.scalars().all()

        if not branchdata_list:
            continue

        metrics_names = await _load_metrics_names(db, branchdata_list)

        message, new_values = await _calculate_new_values(
            bid, branchdata_list, db, metrics_names
        )
        await _apply_updates(branchdata_list, new_values, message, db)

        # Повторный пересчёт после обновления
        message, new_values = await _calculate_new_values(
            bid, branchdata_list, db, metrics_names
        )
        await _apply_updates(branchdata_list, new_values, message, db)

        if message:
            messages[bid] = message

    return messages

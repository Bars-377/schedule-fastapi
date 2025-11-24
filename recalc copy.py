import json
from datetime import date
from decimal import Decimal

from models import BranchData, Branche
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func, or_, select


async def _update_db(branchdata_list, new_values, message, db: AsyncSession):
    if not message:
        for bd in branchdata_list:
            bd.value = new_values[bd.id]
            db.add(bd)
    else:
        for i, bd in enumerate(branchdata_list):
            if i in (0, 2, 3, 7):
            # if i in (6, 2, 3):
                bd.value = Decimal("0")
                db.add(bd)


async def _calc_metric8(branch_id: int, db: AsyncSession, cache_metric: Decimal) -> Decimal:
    """Вычисление 8-й метрики с учётом прогресса по кварталу"""
    # Получаем минимальную и максимальную даты
    dates_query = await db.execute(
        select(func.min(BranchData.record_date), func.max(BranchData.record_date))
        .where(BranchData.branch_id == branch_id)
    )
    min_date, max_date = dates_query.one()

    if not max_date:
        return Decimal(0)

    # Определяем начало квартала по максимальной дате
    month = max_date.month
    year = max_date.year

    if month in (1, 2, 3):
        start = date(year, 1, 1)
    elif month in (4, 5, 6):
        start = date(year, 4, 1)
    elif month in (7, 8, 9):
        start = date(year, 7, 1)
    else:
        start = date(year, 10, 1)

    # Начало учитывает минимальную дату
    if min_date > start:
        start = min_date

    days_count = (max_date - start).days + 1
    if days_count <= 0:
        return Decimal(0)

    # Считаем сумму метрики 3
    sum_query = await db.execute(
        select(func.sum(BranchData.value))
        .where(
            BranchData.branch_id == branch_id,
            BranchData.metric_id == 3,
            BranchData.record_date.between(start, max_date)
        )
    )
    sum_metric3 = Decimal(sum_query.scalar() or 0)

    # Проверка cache_metric
    if cache_metric == 0:
        return Decimal(0)

    return sum_metric3 / Decimal(days_count) * Decimal(100) / cache_metric


def _calc_metric3(values: list[Decimal]) -> Decimal:
    """Вычисление 3-й метрики: value0 - value4 - value5 - value6"""
    result = values[0] - values[4] - values[5] - values[6]
    if result < 0:
        raise ValueError("Отрицательное вычисление")
    return Decimal(result)


def _calc_metric4(values: list[Decimal]) -> Decimal:
    """Вычисление 4-й метрики: (value2 * 100) / value0"""
    if values[0] == 0:
        raise ValueError("Делить на 0 нельзя")
    return Decimal(values[2] * 100 / values[0])


async def _calculate_new_values(branchdata_id: int, branchdata_list: list, db: AsyncSession, branchdata_list_metric: dict):
    new_values = {}
    message = None

    cache_metric = branchdata_list[0].value

    original_values = [Decimal(bd.value) for bd in branchdata_list]

    for bd in branchdata_list:
        if bd.id == branchdata_id:
            new_values[bd.id] = bd.value
            continue

        try:
            if bd.metric_id == 3:  # 3-я метрика
                new_values[bd.id] = _calc_metric3(original_values)
                # cache = new_values[bd.id]
            elif bd.metric_id == 4:  # 4-я метрика
                new_values[bd.id] = _calc_metric4(original_values)
            elif bd.metric_id == 8:  # 8-я метрика
                new_values[bd.id] = await _calc_metric8(bd.branch_id, db, Decimal(cache_metric))
            else:
                new_values[bd.id] = bd.value

        except ValueError as e:
            message = str(e)
            break

    return message, new_values


# --- Логика вычислений для последней даты всех метрик филиала ---
async def recalc(db: AsyncSession, target_date: date, branch_id: int | None = None):
    """
    Пересчет всех метрик филиала для конкретной даты.
    Если branch_id не указан, пересчитывает для всех филиалов.
    """
    branch_ids = [branch_id] if branch_id is not None else []

    # Если branch_id не передан, получаем все уникальные филиалы с данными на target_date
    if not branch_ids:
        # ==============================
        # --- Конфигурация ---
        # ==============================
        with open("config.json", encoding="utf-8") as f:
            config = json.load(f)

        ids_aup = set(map(int, config.get("ids_aup", [])))  # Приводим к int

        result = await db.execute(
            select(Branche.id)
            .where(or_(Branche.department_id.is_(None), ~Branche.department_id.in_(ids_aup)))
            .distinct()
        )
        branch_ids = [row[0] for row in result.all()]

    messages = {}

    branchdata_list_metric = {}

    # print(branch_ids)

    for bid in branch_ids:
        query = await db.execute(
            select(BranchData)
            .where(
                BranchData.branch_id == bid,
                BranchData.record_date == target_date
            )
            .order_by(BranchData.metric_id)
        )
        branchdata_list = query.scalars().all()

        if not branchdata_list:
            continue

        message, new_values = await _calculate_new_values(bid, branchdata_list, db, branchdata_list_metric)
        await _update_db(branchdata_list, new_values, message, db)
        message, new_values = await _calculate_new_values(bid, branchdata_list, db, branchdata_list_metric)
        await _update_db(branchdata_list, new_values, message, db)

        messages[bid] = message

    return {k: v for k, v in messages.items() if v is not None}

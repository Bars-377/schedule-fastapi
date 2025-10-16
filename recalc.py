from decimal import Decimal

from models import BranchData
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select


async def _update_db(branchdata_list, new_values, message, db: AsyncSession):
    if not message:
        for bd in branchdata_list:
            bd.value = new_values[bd.id]
            db.add(bd)
    else:
        for i, bd in enumerate(branchdata_list):
            if i in (0, 2, 3):
                bd.value = Decimal("0")
    await db.commit()


async def _fetch_branchdata(branch_id: int, record_date, db: AsyncSession):
    result = await db.execute(
        select(BranchData)
        .where(
            BranchData.branch_id == branch_id,
            BranchData.record_date == record_date,
        )
        .order_by(BranchData.metric_id)
    )
    return result.scalars().all()


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


def _calculate_new_values(branchdata: BranchData, branchdata_list, original_values):
    new_values = {}
    message = None

    for i, bd in enumerate(branchdata_list):
        if bd.id == branchdata.id:
            new_values[bd.id] = bd.value
            continue

        try:
            if i == 2:  # 3-я метрика
                new_values[bd.id] = _calc_metric3(original_values)
            elif i == 3:  # 4-я метрика
                new_values[bd.id] = _calc_metric4(original_values)
            else:
                new_values[bd.id] = bd.value

        except ValueError as e:
            message = str(e)
            break

    return message, new_values


# --- Логика вычислений для последней даты всех метрик филиала ---
async def recalc(branchdata: BranchData, db: AsyncSession):
    """
    Пересчет всех метрик филиала для последней даты на основе
    BranchData.value, без изменения branchdata до успешного расчета.
    """
    branchdata_list = await _fetch_branchdata(branchdata.branch_id, branchdata.record_date, db)
    if not branchdata_list:
        return None

    original_values = [Decimal(bd.value) for bd in branchdata_list]

    message, new_values = _calculate_new_values(branchdata, branchdata_list, original_values)

    await _update_db(branchdata_list, new_values, message, db)

    return message

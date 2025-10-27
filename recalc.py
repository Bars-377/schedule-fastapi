from datetime import date
from decimal import Decimal

from models import BranchData
from sqlalchemy.ext.asyncio import AsyncSession


async def _update_db(branchdata_list, new_values, message, db: AsyncSession):
    if not message:
        for bd in branchdata_list:
            bd.value = new_values[bd.id]
            db.add(bd)
    else:
        for i, bd in enumerate(branchdata_list):
            if i in (0, 2, 3):
                bd.value = Decimal("0")
                db.add(bd)


# async def _fetch_branchdata(branch_id: int, record_date, db: AsyncSession):
#     result = await db.execute(
#         select(BranchData)
#         .where(
#             BranchData.branch_id == branch_id,
#             BranchData.record_date == record_date,
#         )
#         .order_by(BranchData.metric_id)
#     )
#     return result.scalars().all()


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


def _calc_metric5(value: Decimal, record_date: date) -> Decimal:
    """Вычисление 8-й метрики с учётом прогресса по кварталу"""

    # Определяем квартал по дате
    month = record_date.month
    year = record_date.year

    if month in (1, 2, 3):  # 1 квартал
        start = date(year, 1, 1)
    elif month in (4, 5, 6):  # 2 квартал
        start = date(year, 4, 1)
    elif month in (7, 8, 9):  # 3 квартал
        start = date(year, 7, 1)
    else:  # 4 квартал
        start = date(year, 10, 1)

    # if month in (1, 2, 3):  # 1 квартал
    #     start, end = date(year, 1, 1), date(year, 3, 31)
    # elif month in (4, 5, 6):  # 2 квартал
    #     start, end = date(year, 4, 1), date(year, 6, 30)
    # elif month in (7, 8, 9):  # 3 квартал
    #     start, end = date(year, 7, 1), date(year, 9, 30)
    # else:  # 4 квартал
    #     start, end = date(year, 10, 1), date(year, 12, 31)

    # # Общее количество дней в квартале
    # total_days_in_quarter = (end - start).days + 1

    # Количество прошедших дней в квартале (включая текущий)
    days_passed = (record_date - start).days + 1

    # # Доля прошедших дней квартала
    # progress_ratio = Decimal(days_passed) / Decimal(total_days_in_quarter)

    # # Корректируем values[2] пропорционально прошедшему времени квартала
    # return Decimal(values[2] * progress_ratio)

    # print(values[2])
    # print(days_passed)

    # Корректируем values[2] пропорционально прошедшему времени квартала
    return Decimal(value / days_passed)


def _calculate_new_values(branchdata: BranchData, branchdata_list: list, original_values: list):
    new_values = {}
    message = None

    for bd in branchdata_list:
        if bd.id == branchdata.id:
            new_values[bd.id] = bd.value
            continue

        try:
            if bd.metric_id == 3:  # 3-я метрика
                new_values[bd.id] = _calc_metric3(original_values)
                # cache = new_values[bd.id]
            elif bd.metric_id == 4:  # 4-я метрика
                new_values[bd.id] = _calc_metric4(original_values)
            elif bd.metric_id == 8:  # 8-я метрика
                new_values[bd.id] = _calc_metric5(_calc_metric3(original_values), bd.record_date)
            else:
                new_values[bd.id] = bd.value

        except ValueError as e:
            message = str(e)
            break

    return message, new_values


# --- Логика вычислений для последней даты всех метрик филиала ---
async def recalc(branchdata: BranchData, db: AsyncSession, branchdata_list: list):
    """
    Пересчет всех метрик филиала для последней даты на основе
    BranchData.value, без изменения branchdata до успешного расчета.
    """
    # branchdata_list = await _fetch_branchdata(branchdata.branch_id, branchdata.record_date, db)
    if not branchdata_list:
        return None

    original_values = [Decimal(bd.value) for bd in branchdata_list]

    message, new_values = _calculate_new_values(branchdata, branchdata_list, original_values)

    await _update_db(branchdata_list, new_values, message, db)

    return message

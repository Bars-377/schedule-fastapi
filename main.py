import asyncio
import json
import os
import re
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from lifespan_manager import lifespan
from models import AsyncSessionLocal, BranchData, Branche, Metric, User
from passlib.context import CryptContext
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from starlette.middleware.sessions import SessionMiddleware

app = FastAPI(lifespan=lifespan)

secret_key = os.getenv("SESSION_SECRET_KEY", "fallback_secret_key")
app.add_middleware(SessionMiddleware, secret_key=secret_key)

# Подключаем папку static
BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# --- Шаблоны ---
templates = Jinja2Templates(directory="templates")

# --- Хеширование паролей ---
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


async def get_password_hash(password: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, pwd_context.hash, password)


async def verify_password(plain_password: str, hashed_password: str) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, pwd_context.verify, plain_password, hashed_password
    )


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def get_user(db: AsyncSession, username: str):
    result = await db.execute(select(User).where(User.username == username))
    return result.scalar_one_or_none()


# async def get_current_user(
#     request: Request, db: AsyncSession = Depends(get_db)
# ):
#     username = request.session.get("user")
#     if not username:
#         return None
#     return await get_user(db, username)


async def get_current_user(
    request: Request, db: AsyncSession = Depends(get_db)
):
    username = request.session.get("user")
    if username:
        user = await get_user(db, username)
        if user:
            return user
    # Если нет авторизации, возвращаем гостя
    guest_user = await get_user(db, "Guest")
    if guest_user:
        # Убираем права редактирования
        guest_user.can_edit = 0
    return guest_user


async def require_login(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(status_code=401, detail="Не авторизован")
    return user


async def require_edit_permission(user=Depends(require_login)):
    if user.can_edit != 1:
        raise HTTPException(
            status_code=403, detail="Нет прав на редактирование"
        )
    return user


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


# Чтение конфигурации
CONFIG_PATH = BASE_DIR / "config.json"
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
else:
    config = {"show_all_on_first_page": False}


async def fetch_branches(db: AsyncSession, page: int, per_page: int = 5):
    # Проверка, нужно ли отображать все на первой странице
    if config.get("show_all_on_first_page", False) and page == 1:
        result = await db.execute(select(Branche).order_by(Branche.id))
        branches = result.scalars().all()
        total_branches = branches
        total_pages = 1
        start_page = 1
        end_page = 1
        return branches, total_branches, total_pages, start_page, end_page

    # Обычная логика с пагинацией
    result = await db.execute(
        select(Branche)
        .order_by(Branche.id)
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    branches = result.scalars().all()

    total_branches_result = await db.execute(select(Branche))
    total_branches = total_branches_result.scalars().all()
    total_pages = (len(total_branches) + per_page - 1) // per_page

    start_page = max(page - 1, 1)
    end_page = min(page + 1, total_pages)

    return branches, total_branches, total_pages, start_page, end_page


async def fetch_metrics(db: AsyncSession):
    result = await db.execute(select(Metric).order_by(Metric.id))
    return result.scalars().all()

async def fetch_branchdata(db: AsyncSession):
    result = await db.execute(select(BranchData))
    return result.scalars().all()

def build_latest_data(branchdata_rows: list[BranchData]) -> dict[tuple[int, int], BranchData]:
    latest_data = {}
    for bd in branchdata_rows:
        key = (bd.branch_id, bd.metric_id)
        if key not in latest_data or bd.record_date > latest_data[key].record_date:
            latest_data[key] = bd
    return latest_data

def build_table_data(branches: list[Branche], metrics: list[Metric], latest_data: dict[tuple[int, int], BranchData], ids_aup: set[int]):
    table_data = []

    for branch in branches:
        if branch.department_id in ids_aup:
            continue  # пропускаем ветки с запрещёнными department_id

        row = {
            "branch_id": branch.id,
            "branch_name": branch.name,
            "metrics": {},
        }
        for metric in metrics:
            bd = latest_data.get((branch.id, metric.id))
            if bd:
                row["metrics"][metric.name] = {
                    "id": bd.id,
                    "value": bd.value,
                    "record_date": bd.record_date,
                    "metric_id": metric.id,
                }
            else:
                row["metrics"][metric.name] = {
                    "id": None,
                    "value": 0,
                    "record_date": None,
                    "metric_id": metric.id,
                }
        table_data.append(row)
    return table_data


def calculate_totals(table_data: list[dict], metrics: list[Metric]):
    totals = {}

    for metric in metrics:
        total = Decimal("0.00")
        for row in table_data:
            metric_info = row["metrics"].get(metric.name)
            if metric_info and metric_info["value"] is not None:
                total += Decimal(str(metric_info["value"]))
        total = total.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
        totals[metric.name] = total

    return totals


def find_latest_date(branchdata_rows: list[BranchData]):
    latest_date = None
    for bd in branchdata_rows:
        if bd.record_date:
            if latest_date is None or bd.record_date > latest_date:
                latest_date = bd.record_date
    return latest_date

# --- Основная функция ---
async def get_page_data(request: Request, page: int, db: AsyncSession, user: Depends, msg: str):
    branches, total_branches, total_pages, start_page, end_page = await fetch_branches(db, page)
    metrics = await fetch_metrics(db)
    branchdata_rows = await fetch_branchdata(db)

    latest_data = build_latest_data(branchdata_rows)
    ids_aup = (1, 31, 2, 29, 28, 15, 21, 4, 25, 26, 27, 24, 3, 23, 16, 20, 61, 17, 18)
    table_data = build_table_data(branches, metrics, latest_data, ids_aup)
    totals = calculate_totals(table_data, metrics)
    latest_date = find_latest_date(branchdata_rows)

    # передаем список редактируемых метрик из конфига
    editing_metrics = config.get("editing_metrics", ())

    return {
        "request": request,
        "user": user,
        "metrics": metrics,
        "table_data": table_data,
        "page": page,
        "total_pages": total_pages,
        "start_page": start_page,
        "end_page": end_page,
        "msg": msg,
        "latest_date": latest_date,
        "totals": totals,
        "editing": editing_metrics,
    }


# --- Главная страница с возможностью отображения сообщения ---
@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    page: int = 1,
    db: AsyncSession = Depends(get_db),
    user=Depends(require_login),
    msg: str = None,
):
    data = await get_page_data(request, page, db, user, msg)

    return templates.TemplateResponse("table.html", {**data})


# --- Добавление нового филиала ---
@app.post("/add")
async def add_branch(
    name: str = Form(...),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    if not name.strip():
        return HTMLResponse(
            "<h3>Имя филиала не может быть пустым</h3><a href='/'>Назад</a>"
        )

    new_branch = Branche(name=name.strip())
    db.add(new_branch)
    await db.commit()
    await db.refresh(new_branch)

    # --- Создание пустых записей branchdata для всех метрик на последнюю дату ---
    metrics = (
        (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    )
    for metric in metrics:
        last_date_metric = await get_last_date(db, new_branch.id, metric.id)
        result = await db.execute(
            select(BranchData).where(
                BranchData.branch_id == new_branch.id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == last_date_metric,
            )
        )
        if not result.scalar_one_or_none():
            new_bd = BranchData(
                branch_id=new_branch.id,
                metric_id=metric.id,
                record_date=last_date_metric,
                value=0.0,
            )
            db.add(new_bd)

    await db.commit()

    # --- Переход на последнюю страницу branchdata ---
    per_page = 5
    total_rows = (await db.execute(select(Branche))).scalars().all()
    last_page = (len(total_rows) + per_page - 1) // per_page
    last_page = last_page if last_page > 0 else 1

    return RedirectResponse(
        f"/?page={last_page}&msg=Отдел+добавлен&status=200", status_code=303
    )


# --- Получение последней даты branchdata для ветки и метрики ---
async def get_last_date(db: AsyncSession, branch_id: int, metric_id: int):
    result = await db.execute(
        select(func.max(BranchData.record_date)).where(
            BranchData.branch_id == branch_id,
            BranchData.metric_id == metric_id,
        )
    )
    last_date = result.scalar_one_or_none()
    return last_date or date.today()


def parse_number(value: str) -> Decimal:
    if not value:
        return 0.0
    # Убираем пробелы
    value = value.strip()
    # Заменяем запятую на точку
    value = value.replace(",", ".")
    # Проверяем, есть ли число с помощью регулярки
    if re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", value):
        return Decimal(value)
    return 0.0


# --- Обновление данных филиала через branchdata ---
@app.post("/update/{row_id}")
async def update_branchdata(
    row_id: str,
    value: str = Form("0"),
    branch_id: int = Form(...),
    metric_id: int = Form(...),
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    value = parse_number(value)
    today = date.today()

    # --- Получаем последнюю дату для этой ветки, кроме сегодняшней ---
    result = await db.execute(
        select(func.max(BranchData.record_date)).where(
            BranchData.branch_id == branch_id,
            BranchData.record_date < today
        )
    )
    last_date = result.scalar_one_or_none() or today

    # --- Проверяем, есть ли запись на сегодня для конкретной метрики ---
    result = await db.execute(
        select(BranchData).where(
            BranchData.branch_id == branch_id,
            BranchData.metric_id == metric_id,
            BranchData.record_date == today,
        )
    )
    branchdata = result.scalar_one_or_none()

    if branchdata:
        # --- Обновляем существующую запись за сегодня ---
        branchdata.value = Decimal(value)
        db.add(branchdata)
        await db.commit()
        await db.refresh(branchdata)
    else:
        # --- Создаём новую запись с сегодняшней датой ---
        branchdata = BranchData(
            branch_id=branch_id,
            metric_id=metric_id,
            record_date=today,
            value=Decimal(value),
        )
        db.add(branchdata)
        await db.commit()
        await db.refresh(branchdata)

    # --- Создаём недостающие branchdata для всех метрик на сегодняшнюю дату, копируя значения с последней даты ---
    metrics = (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    for metric in metrics:
        result = await db.execute(
            select(BranchData).where(
                BranchData.branch_id == branch_id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == today,
            )
        )
        existing_bd = result.scalar_one_or_none()
        if not existing_bd:
            # Берем значение с последней даты
            result = await db.execute(
                select(BranchData).where(
                    BranchData.branch_id == branch_id,
                    BranchData.metric_id == metric.id,
                    BranchData.record_date == last_date,
                )
            )
            last_bd = result.scalar_one_or_none()
            new_value = last_bd.value if last_bd else Decimal("0.0")

            new_bd = BranchData(
                branch_id=branch_id,
                metric_id=metric.id,
                record_date=today,
                value=new_value,
            )
            db.add(new_bd)
    await db.commit()

    # --- Пересчёт метрик для филиала ---
    message = await recalc(branchdata, db)
    await db.refresh(branchdata)

    if message:
        return RedirectResponse(f"/?page={page}&msg={message}&status=500", status_code=303)

    return RedirectResponse(f"/?page={page}&msg=Сохранено&status=200", status_code=303)


# --- Удаление филиала ---
@app.post("/delete/{row_id}")
async def delete_branch(
    row_id: int,
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    branch = await db.get(Branche, row_id)
    if branch:
        # Удаляем все данные branchdata для этого филиала
        await db.execute(
            BranchData.__table__.delete().where(BranchData.branch_id == row_id)
        )
        # Удаляем сам филиал
        await db.delete(branch)
        await db.commit()

    # --- Пересчёт страниц branchdata после удаления ---
    per_page = 5
    total_rows = (await db.execute(select(Branche))).scalars().all()
    total_pages = (len(total_rows) + per_page - 1) // per_page
    if page > total_pages:
        page = total_pages if total_pages > 0 else 1

    return RedirectResponse(f"/?page={page}&msg=Удалено&status=200", status_code=303)


@app.get("/register_form", response_class=HTMLResponse)
async def register_form(request: Request):
    return templates.TemplateResponse(
        "register_form.html", {"request": request}
    )


# --- Регистрация пользователя с перенаправлением на главную страницу ---
@app.post("/register")
async def register(
    username: str = Form(...),
    password: str = Form(...),
    can_edit: int = Form(0),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    # --- Проверка username ---
    if len(username) < 3:
        return HTMLResponse(
            "<h3>Имя пользователя должно быть не менее 3 символов</h3> \
            <a href='/register_form'>Назад</a>"
        )
    if not re.fullmatch(r"[A-Za-z0-9_]+", username):
        return HTMLResponse(
            "<h3>Имя пользователя может содержать только английские буквы, \
            цифры и _</h3><a href='/register_form'>Назад</a>"
        )

    # --- Проверка password ---
    if len(password) < 6:
        return HTMLResponse(
            "<h3>Пароль должен быть не менее 6 символов</h3> \
            <a href='/register_form'>Назад</a>"
        )

    if not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password):
        return HTMLResponse(
            "<h3>Пароль должен содержать хотя бы одну букву и одну цифру</h3> \
            <a href='/register_form'>Назад</a>"
        )

    # --- Проверка существующего пользователя ---
    if await get_user(db, username):
        return HTMLResponse(
            "<h3>Пользователь уже существует</h3> \
            <a href='/register_form'>Назад</a>"
        )

    # --- Создание пользователя ---
    user = User(
        username=username,
        hashed_password=await get_password_hash(password),
        can_edit=can_edit,
    )
    db.add(user)
    await db.commit()

    # --- Сразу авторизуем пользователя ---
    request.session["user"] = user.username

    # --- Перенаправление на главную с сообщением ---
    return RedirectResponse(url="/?msg=Регистрация+успешна&status=200", status_code=303)


@app.get("/login_form", response_class=HTMLResponse)
async def login_form(request: Request):
    return templates.TemplateResponse(
        "auth_form.html",
        {
            "request": request,
            "title": "Авторизация пользователя",
            "action": "/login",
            "submit_text": "Войти",
            "register_link": True,
        },
    )


@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    user = await get_user(db, username)
    if not user or not await verify_password(password, user.hashed_password):
        return HTMLResponse(
            "<h3>Неверный логин или пароль</h3><a href='/login_form'>Назад</a>"
        )
    request.session["user"] = user.username
    return RedirectResponse("/", status_code=303)


# @app.get("/logout")
# async def logout(request: Request):
#     request.session.clear()
#     return RedirectResponse("/login_form", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    # Ставим пользователя Guest
    request.session["user"] = "Guest"
    return RedirectResponse("/", status_code=303)


@app.exception_handler(HTTPException)
async def auth_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code == 401:
        return RedirectResponse(url="/login_form")
    return HTMLResponse(content=str(exc.detail), status_code=exc.status_code)


# --- Добавление новой метрики ---
@app.post("/add_metric")
async def add_metric(
    name: str = Form(...),
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    if not name.strip():
        return RedirectResponse(
            f"/?page={page}&msg=Имя+метрики+не+может+быть+пустым&status=500",
            status_code=303,
        )

    existing = await db.execute(
        select(Metric).where(Metric.name == name.strip())
    )
    if existing.scalar_one_or_none():
        return RedirectResponse(
            f"/?page={page}&msg=Метрика+с+таким+именем+уже+существует&status=500",
            status_code=303,
        )

    new_metric = Metric(name=name.strip())
    db.add(new_metric)
    await db.commit()
    await db.refresh(new_metric)

    # Создаем branchdata для всех филиалов на последнюю дату
    branches = (
        (await db.execute(select(Branche).order_by(Branche.id)))
        .scalars()
        .all()
    )
    for branch in branches:
        last_date_branch = await get_last_date(db, branch.id, new_metric.id)
        result = await db.execute(
            select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == new_metric.id,
                BranchData.record_date == last_date_branch,
            )
        )
        if not result.scalar_one_or_none():
            new_bd = BranchData(
                branch_id=branch.id,
                metric_id=new_metric.id,
                record_date=last_date_branch,
                value=0.0,
            )
            db.add(new_bd)

    await db.commit()
    return RedirectResponse(
        f"/?page={page}&msg=Метрика+добавлена&status=200", status_code=303
    )


# --- Удаление метрики ---
@app.post("/delete_metric/{metric_id}")
async def delete_metric(
    metric_id: int,
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    metric = await db.get(Metric, metric_id)
    if metric:
        # Удаляем все данные branchdata для этой метрики
        await db.execute(
            BranchData.__table__.delete().where(
                BranchData.metric_id == metric_id
            )
        )
        # Удаляем саму метрику
        await db.delete(metric)
        await db.commit()
    return RedirectResponse(
        f"/?page={page}&msg=Метрика+удалена&status=200", status_code=303
    )


# --- Обновление ID метрики (только для админа) ---
@app.post("/update_metric_id/{metric_id}")
async def update_metric_id(
    metric_id: int,
    new_id: int = Form(...),
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    # Проверка, существует ли новая метрика с таким id
    existing_metric = await db.get(Metric, new_id)
    if existing_metric:
        return RedirectResponse(
            f"/?page={page}&msg=Метрика+с+таким+ID+уже+существует&status=500",
            status_code=303,
        )

    metric = await db.get(Metric, metric_id)
    if not metric:
        return RedirectResponse(
            f"/?page={page}&msg=Метрика+не+найдена&status=500",
            status_code=303,
        )

    # Обновляем все связанные BranchData
    await db.execute(
        BranchData.__table__.update()
        .where(BranchData.metric_id == metric.id)
        .values(metric_id=new_id)
    )

    # Обновляем саму метрику
    metric.id = new_id
    db.add(metric)
    await db.commit()

    return RedirectResponse(
        f"/?page={page}&msg=ID+метрики+обновлен&status=200",
        status_code=303,
    )


# --- Обновление ID отдела (только для админа) ---
@app.post("/update_branch_id/{branch_id}")
async def update_branch_id(
    branch_id: int,
    new_id: int = Form(...),
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission),
):
    # Проверка, существует ли отдел с новым ID
    existing_branch = await db.get(Branche, new_id)
    if existing_branch:
        return RedirectResponse(
            f"/?page={page}&msg=Отдел+с+таким+ID+уже+существует&status=500",
            status_code=303,
        )

    branch = await db.get(Branche, branch_id)
    if not branch:
        return RedirectResponse(
            f"/?page={page}&msg=Отдел+не+найден&status=500",
            status_code=303,
        )

    # Обновляем все связанные BranchData
    await db.execute(
        BranchData.__table__.update()
        .where(BranchData.branch_id == branch.id)
        .values(branch_id=new_id)
    )

    # Обновляем сам отдел
    branch.id = new_id
    db.add(branch)
    await db.commit()

    return RedirectResponse(
        f"/?page={page}&msg=ID+отдела+обновлен&status=200",
        status_code=303,
    )


# --- Запуск сервера ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8444, reload=True)

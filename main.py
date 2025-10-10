import asyncio
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from starlette.middleware.sessions import SessionMiddleware

from lifespan_manager import lifespan
from models import AsyncSessionLocal, BranchData, Branche, Metric, User

app = FastAPI(lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key="supersecretkey")

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


async def get_current_user(
    request: Request, db: AsyncSession = Depends(get_db)
):
    username = request.session.get("user")
    if not username:
        return None
    return await get_user(db, username)


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
    BranchData.value, но сам branchdata не меняется.
    """

    # Получаем все branchdata для этого филиала с последней датой
    result = await db.execute(
        select(BranchData)
        .where(
            BranchData.branch_id == branchdata.branch_id,
            BranchData.record_date == branchdata.record_date,
        )
        .order_by(BranchData.metric_id)  # сортировка по id
    )
    latest_branchdata = result.scalars().all()

    print(latest_branchdata)

    latest_branchdata[1].value = float(Decimal("16"))
    latest_branchdata[4].value = float(Decimal("6"))
    latest_branchdata[5].value = float(Decimal("2"))
    # latest_branchdata[6].value = float(Decimal("7.6"))
    latest_branchdata[7].value = float(Decimal("72.2"))

    for i, bd in enumerate(latest_branchdata):
        if bd.id == branchdata.id:
            # Сам branchdata не трогаем
            continue

        # --- Формулы зависят от позиции bd в списке ---
        if i == 2:
            bd.value = float(
                Decimal(latest_branchdata[0].value)
                - Decimal(latest_branchdata[4].value)
                - Decimal(latest_branchdata[5].value)
                - Decimal(latest_branchdata[6].value)
            )
        elif i == 3 and latest_branchdata[0].value != 0:
            bd.value = float(
                Decimal(latest_branchdata[2].value)
                * Decimal("100")
                / Decimal(latest_branchdata[0].value)
            )
        # else:
        #     # Для всех остальных
        #     bd.value = float(Decimal(latest_branchdata[0].value) \
        #          * Decimal('2'))
        elif i != 6:
            bd.value = float(Decimal("0"))

        db.add(bd)

    await db.commit()


# --- Главная страница с возможностью отображения сообщения ---
@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    page: int = 1,
    db: AsyncSession = Depends(get_db),
    user=Depends(require_login),
    msg: str = None,
):
    per_page = 5  # количество филиалов на страницу

    # --- Получаем все филиалы с пагинацией ---
    result = await db.execute(
        select(Branche)
        .order_by(Branche.id)
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    branches = result.scalars().all()

    total_branches = (await db.execute(select(Branche))).scalars().all()
    total_pages = (len(total_branches) + per_page - 1) // per_page
    start_page = max(page - 1, 1)
    end_page = min(page + 1, total_pages)

    # --- Получаем все метрики ---
    metrics = (
        (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    )

    # --- Получаем все branchdata ---
    branchdata_rows = (await db.execute(select(BranchData))).scalars().all()

    # --- Строим словарь последней записи для каждой пары
    # (branch_id, metric_id) ---
    latest_data = {}
    for bd in branchdata_rows:
        key = (bd.branch_id, bd.metric_id)
        if (
            key not in latest_data
            or bd.record_date > latest_data[key].record_date
        ):
            latest_data[key] = bd

    # Формируем table_data
    table_data = []
    for branch in branches:
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
                    "value": "",
                    "record_date": None,
                    "metric_id": metric.id,
                }
        table_data.append(row)

    # Находим самую актуальную дату среди всех branchdata
    latest_date = None
    for bd in branchdata_rows:
        if bd.record_date:
            if latest_date is None or bd.record_date > latest_date:
                latest_date = bd.record_date

    return templates.TemplateResponse(
        "table.html",
        {
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
        },
    )


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
        f"/?page={last_page}&msg=Отдел+добавлен", status_code=303
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


def parse_number(value: str) -> float:
    if not value:
        return 0.0
    # Убираем пробелы
    value = value.strip()
    # Заменяем запятую на точку
    value = value.replace(",", ".")
    # Проверяем, есть ли число с помощью регулярки
    if re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", value):
        return float(value)
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

    # Использование:
    value = parse_number(value)

    last_date = await get_last_date(db, branch_id, metric_id)

    # --- Обновляем или создаём текущую запись ---
    result = await db.execute(
        select(BranchData).where(
            BranchData.branch_id == branch_id,
            BranchData.metric_id == metric_id,
            BranchData.record_date == last_date,
        )
    )
    branchdata = result.scalar_one_or_none()

    if branchdata:
        branchdata.value = Decimal(value)
        db.add(branchdata)
        await db.commit()
    else:
        branchdata = BranchData(
            branch_id=branch_id,
            metric_id=metric_id,
            record_date=last_date,
            value=Decimal(value),
        )
        db.add(branchdata)
        await db.commit()
        await db.refresh(branchdata)

    # --- Создаём недостающие branchdata для новых метрик на последнюю дату ---
    metrics = (
        (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    )
    for metric in metrics:
        last_date_metric = await get_last_date(db, branch_id, metric.id)
        result = await db.execute(
            select(BranchData).where(
                BranchData.branch_id == branch_id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == last_date_metric,
            )
        )
        if not result.scalar_one_or_none():
            new_bd = BranchData(
                branch_id=branch_id,
                metric_id=metric.id,
                record_date=last_date_metric,
                value=0.0,
            )
            db.add(new_bd)
    await db.commit()

    # --- Пересчёт всех метрик филиала ---
    await recalc(branchdata, db)

    await db.refresh(branchdata)
    return RedirectResponse(f"/?page={page}&msg=Сохранено", status_code=303)


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

    return RedirectResponse(f"/?page={page}&msg=Удалено", status_code=303)


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
    return RedirectResponse(url="/?msg=Регистрация+успешна", status_code=303)


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


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login_form", status_code=303)


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
            f"/?page={page}&msg=Имя+метрики+не+может+быть+пустым",
            status_code=303,
        )

    existing = await db.execute(
        select(Metric).where(Metric.name == name.strip())
    )
    if existing.scalar_one_or_none():
        return RedirectResponse(
            f"/?page={page}&msg=Метрика+с+таким+именем+уже+существует",
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
        f"/?page={page}&msg=Метрика+добавлена", status_code=303
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
        f"/?page={page}&msg=Метрика+удалена", status_code=303
    )


# --- Запуск сервера ---
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

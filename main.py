import json
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import SQLModel, Field, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import UniqueConstraint, Column, Integer
from passlib.context import CryptContext
from datetime import date
from decimal import Decimal
import asyncio
from contextlib import asynccontextmanager
import re
from pathlib import Path
from typing import Optional

# --- Чтение конфигурации ---
with open("config.json", "r") as f:
    config = json.load(f)

# --- БД ---
DB_USER = config["DB_USER"]
DB_PASSWORD = config["DB_PASSWORD"]
DB_HOST = config["DB_HOST"]
DB_NAME = config["DB_NAME"]

DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


# --- Модели ---
class User(SQLModel, table=True):
    __tablename__ = "users"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True))
    username: str = Field(default="")
    hashed_password: str = Field(default="")
    can_edit: int = Field(default=0)

class Branche(SQLModel, table=True):
    __tablename__ = "branches"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True))
    name: str = Field(default="")

class Metric(SQLModel, table=True):
    __tablename__ = "metrics"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True))
    name: str = Field(default="")

class BranchData(SQLModel, table=True):
    __tablename__ = "branch_data"
    __table_args__ = (UniqueConstraint("branch_id", "metric_id", "record_date", name="branch_metric_date_unique"),)
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True))
    branch_id: int = Field(default=0)
    metric_id: int = Field(default=0)
    record_date: date = Field(default_factory=date.today)
    value: float = Field(default=0.0)

# --- FastAPI с lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    yield
    await engine.dispose()

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
    return await loop.run_in_executor(None, pwd_context.verify, plain_password, hashed_password)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

async def get_user(db: AsyncSession, username: str):
    result = await db.execute(select(User).where(User.username == username))
    return result.scalar_one_or_none()

async def get_current_user(request: Request, db: AsyncSession = Depends(get_db)):
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
        raise HTTPException(status_code=403, detail="Нет прав на редактирование")
    return user

# --- Логика вычислений для последней даты всех метрик филиала ---
async def recalc(branch_data: BranchData, db: AsyncSession):
    """
    Пересчет всех метрик филиала для последней даты на основе branch_data.value,
    но сам branch_data не меняется.
    """
    today = date.today()

    # Получаем все branch_data для этого филиала с последней датой
    result = await db.execute(
        select(BranchData).where(
            BranchData.branch_id == branch_data.branch_id,
            BranchData.record_date == today
        )
        .order_by(BranchData.id)  # сортировка по id
    )
    latest_branch_data = result.scalars().all()

    for i, bd in enumerate(latest_branch_data):
        if bd.id == branch_data.id:
            # Сам branch_data не трогаем
            continue

        # --- Формулы зависят от позиции bd в списке ---
        if i == 1:
            bd.value = float(Decimal(branch_data.value) * Decimal('2') + Decimal('5'))
        elif i == 2:
            bd.value = float(Decimal(branch_data.value) * Decimal('1.5') + Decimal('10'))
        else:
            # Для всех остальных
            bd.value = float(Decimal(branch_data.value) * Decimal('2'))

        db.add(bd)

    await db.commit()

# --- Главная страница с возможностью отображения сообщения ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, page: int = 1, db: AsyncSession = Depends(get_db), user=Depends(require_login), msg: str = None):
    per_page = 5  # количество филиалов на страницу

    # --- Получаем все филиалы с пагинацией ---
    result = await db.execute(select(Branche).offset((page - 1) * per_page).limit(per_page))
    branches = result.scalars().all()

    total_branches = (await db.execute(select(Branche))).scalars().all()
    total_pages = (len(total_branches) + per_page - 1) // per_page
    start_page = max(page - 1, 1)
    end_page = min(page + 1, total_pages)

    # --- Получаем все метрики ---
    metrics = (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()

    # --- Получаем все branch_data ---
    branch_data_rows = (await db.execute(select(BranchData))).scalars().all()

    # --- Строим словарь последней записи для каждой пары (branch_id, metric_id) ---
    latest_data = {}
    for bd in branch_data_rows:
        key = (bd.branch_id, bd.metric_id)
        if key not in latest_data or bd.record_date > latest_data[key].record_date:
            latest_data[key] = bd

    # Формируем table_data
    table_data = []
    for branch in branches:
        row = {"branch_id": branch.id, "branch_name": branch.name, "metrics": {}}
        for metric in metrics:
            bd = latest_data.get((branch.id, metric.id))
            if bd:
                row["metrics"][metric.name] = {
                    "id": bd.id,
                    "value": bd.value,
                    "record_date": bd.record_date,
                    "metric_id": metric.id
                }
            else:
                row["metrics"][metric.name] = {
                    "id": None,
                    "value": "",
                    "record_date": None,
                    "metric_id": metric.id
                }
        table_data.append(row)

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
            "msg": msg
        }
    )

# --- Добавление нового филиала ---
@app.post("/add")
async def add_branch(name: str = Form(...), db: AsyncSession = Depends(get_db), user=Depends(require_edit_permission)):
    if not name.strip():
        return HTMLResponse("<h3>Имя филиала не может быть пустым</h3><a href='/'>Назад</a>")

    # Создание филиала
    new_branch = Branche(name=name.strip())
    db.add(new_branch)
    await db.commit()
    await db.refresh(new_branch)  # важно, чтобы new_branch.id стал доступен

    # --- Создание пустых записей branch_data для всех метрик ---
    metrics = (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    for metric in metrics:
        # проверяем, есть ли уже запись на сегодня
        result = await db.execute(
            select(BranchData).where(
                BranchData.branch_id == new_branch.id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == date.today()
            )
        )
        existing = result.scalar_one_or_none()
        if not existing:
            new_bd = BranchData(
                branch_id=new_branch.id,
                metric_id=metric.id,
                record_date=date.today(),
                value=0.0  # или можно оставить пустое значение
            )
            db.add(new_bd)

    await db.commit()

    # --- Переход на последнюю страницу BranchData ---
    per_page = 5
    total_rows = (await db.execute(select(Branche))).scalars().all()
    last_page = (len(total_rows) + per_page - 1) // per_page
    last_page = last_page if last_page > 0 else 1

    return RedirectResponse(f"/?page={last_page}&msg=Отдел+добавлен", status_code=303)

# --- Обновление данных филиала через BranchData ---
@app.post("/update/{row_id}")
async def update_branch_data(
    row_id: str,  # строка, чтобы поймать 'new'
    value: float = Form(...),
    branch_id: int = Form(...),
    metric_id: int = Form(...),
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission)
):
    today = date.today()

    # --- Обновляем или создаём текущую запись ---
    result = await db.execute(
        select(BranchData).where(
            BranchData.branch_id == branch_id,
            BranchData.metric_id == metric_id,
            BranchData.record_date == today
        )
    )
    branch_data = result.scalar_one_or_none()

    if branch_data:
        branch_data.value = Decimal(value)
        db.add(branch_data)
        await db.commit()
    else:
        branch_data = BranchData(
            branch_id=branch_id,
            metric_id=metric_id,
            record_date=today,
            value=Decimal(value)
        )
        db.add(branch_data)
        await db.commit()
        await db.refresh(branch_data)

    # --- Создаём недостающие branch_data для новых метрик ---
    metrics = (await db.execute(select(Metric).order_by(Metric.id))).scalars().all()
    for metric in metrics:
        result = await db.execute(
            select(BranchData).where(
                BranchData.branch_id == branch_id,
                BranchData.metric_id == metric.id,
                BranchData.record_date == today
            )
        )
        if not result.scalar_one_or_none():
            new_bd = BranchData(
                branch_id=branch_id,
                metric_id=metric.id,
                record_date=today,
                value=0.0
            )
            db.add(new_bd)
    await db.commit()

    # --- Пересчёт всех метрик филиала ---
    await recalc(branch_data, db)

    await db.refresh(branch_data)
    return RedirectResponse(f"/?page={page}&msg=Сохранено", status_code=303)

# --- Удаление филиала ---
@app.post("/delete/{row_id}")
async def delete_branch(row_id: int, page: int = Form(1), db: AsyncSession = Depends(get_db), user=Depends(require_edit_permission)):
    branch = await db.get(Branche, row_id)
    if branch:
        # Удаляем все данные branch_data для этого филиала
        await db.execute(
            BranchData.__table__.delete().where(BranchData.branch_id == row_id)
        )
        # Удаляем сам филиал
        await db.delete(branch)
        await db.commit()

    # --- Пересчёт страниц BranchData после удаления ---
    per_page = 5
    total_rows = (await db.execute(select(Branche))).scalars().all()
    total_pages = (len(total_rows) + per_page - 1) // per_page
    if page > total_pages:
        page = total_pages if total_pages > 0 else 1

    return RedirectResponse(f"/?page={page}", status_code=303)

@app.get("/register_form", response_class=HTMLResponse)
async def register_form(request: Request):
    return templates.TemplateResponse("register_form.html", {"request": request})

# --- Регистрация пользователя с перенаправлением на главную страницу ---
@app.post("/register")
async def register(
    username: str = Form(...), 
    password: str = Form(...), 
    can_edit: int = Form(0), 
    db: AsyncSession = Depends(get_db),
    request: Request = None
):
    # --- Проверка username ---
    if len(username) < 3:
        return HTMLResponse("<h3>Имя пользователя должно быть не менее 3 символов</h3><a href='/register_form'>Назад</a>")
    if not re.fullmatch(r"[A-Za-z0-9_]+", username):
        return HTMLResponse("<h3>Имя пользователя может содержать только английские буквы, цифры и _</h3><a href='/register_form'>Назад</a>")

    # --- Проверка password ---
    if len(password) < 6:
        return HTMLResponse("<h3>Пароль должен быть не менее 6 символов</h3><a href='/register_form'>Назад</a>")

    if not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password):
        return HTMLResponse("<h3>Пароль должен содержать хотя бы одну букву и одну цифру</h3><a href='/register_form'>Назад</a>")

    # --- Проверка существующего пользователя ---
    if await get_user(db, username):
        return HTMLResponse("<h3>Пользователь уже существует</h3><a href='/register_form'>Назад</a>")

    # --- Создание пользователя ---
    user = User(username=username, hashed_password=await get_password_hash(password), can_edit=can_edit)
    db.add(user)
    await db.commit()

    # --- Сразу авторизуем пользователя ---
    request.session["user"] = user.username

    # --- Перенаправление на главную с сообщением ---
    return RedirectResponse(url="/?msg=Регистрация+успешна", status_code=303)

@app.get("/login_form", response_class=HTMLResponse)
async def login_form(request: Request):
    return templates.TemplateResponse("auth_form.html", {"request": request, "title": "Авторизация пользователя", "action": "/login", "submit_text": "Войти", "register_link": True})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...), db: AsyncSession = Depends(get_db)):
    user = await get_user(db, username)
    if not user or not await verify_password(password, user.hashed_password):
        return HTMLResponse("<h3>Неверный логин или пароль</h3><a href='/login_form'>Назад</a>")
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

# --- Обработка добавления новой метрики ---
@app.post("/add_metric")
async def add_metric(
    name: str = Form(...), 
    page: int = Form(1),  # получаем текущую страницу
    db: AsyncSession = Depends(get_db), 
    user=Depends(require_edit_permission)
):
    if not name.strip():
        return RedirectResponse(f"/?page={page}&msg=Имя+метрики+не+может+быть+пустым", status_code=303)

    # Проверяем существующую метрику
    existing = await db.execute(select(Metric).where(Metric.name == name.strip()))
    if existing.scalar_one_or_none():
        return RedirectResponse(f"/?page={page}&msg=Метрика+с+таким+именем+уже+существует", status_code=303)

    # Создание метрики
    new_metric = Metric(name=name.strip())
    db.add(new_metric)
    await db.commit()
    await db.refresh(new_metric)

    # Создаем BranchData для всех филиалов на сегодня
    branches = (await db.execute(select(Branche))).scalars().all()
    for branch in branches:
        existing_bd = (await db.execute(
            select(BranchData).where(
                BranchData.branch_id == branch.id,
                BranchData.metric_id == new_metric.id,
                BranchData.record_date == date.today()
            )
        )).scalars().first()
        if not existing_bd:
            new_bd = BranchData(
                branch_id=branch.id,
                metric_id=new_metric.id,
                record_date=date.today(),
                value=0.0
            )
            db.add(new_bd)

    await db.commit()
    return RedirectResponse(f"/?page={page}&msg=Метрика+добавлена", status_code=303)

# --- Удаление метрики ---
@app.post("/delete_metric/{metric_id}")
async def delete_metric(
    metric_id: int,
    page: int = Form(1),
    db: AsyncSession = Depends(get_db),
    user=Depends(require_edit_permission)
):
    metric = await db.get(Metric, metric_id)
    if metric:
        # Удаляем все данные branch_data для этой метрики
        await db.execute(
            BranchData.__table__.delete().where(BranchData.metric_id == metric_id)
        )
        # Удаляем саму метрику
        await db.delete(metric)
        await db.commit()
    return RedirectResponse(f"/?page={page}&msg=Метрика+удалена", status_code=303)

# --- Запуск сервера ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

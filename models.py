import json
from datetime import date
from typing import Optional

from sqlalchemy import Column, Integer, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Field, SQLModel

# --- Чтение конфигурации ---
with open("config.json", "r") as f:
    config = json.load(f)

# --- БД ---
DB_USER = config["DB_USER"]
DB_PASSWORD = config["DB_PASSWORD"]
DB_HOST = config["DB_HOST"]
DB_NAME = config["DB_NAME"]

DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)


# --- Модели ---
class User(SQLModel, table=True):
    __tablename__ = "users"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(
        default=None,
        sa_column=Column(Integer, primary_key=True, autoincrement=True),
    )
    username: str = Field(default="")
    hashed_password: str = Field(default="")
    can_edit: int = Field(default=0)


class Branche(SQLModel, table=True):
    __tablename__ = "branches"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(
        default=None,
        sa_column=Column(Integer, primary_key=True, autoincrement=True),
    )
    name: str = Field(default="")

    department_id: int = Field(
        default=0, sa_column=Column(Integer, nullable=False)
    )


class Metric(SQLModel, table=True):
    __tablename__ = "metrics"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(
        default=None,
        sa_column=Column(Integer, primary_key=True, autoincrement=True),
    )
    name: str = Field(default="")


class BranchData(SQLModel, table=True):
    __tablename__ = "branchdata"
    __table_args__ = (
        UniqueConstraint(
            "branch_id",
            "metric_id",
            "record_date",
            name="branch_metric_date_unique",
        ),
    )
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(
        default=None,
        sa_column=Column(Integer, primary_key=True, autoincrement=True),
    )
    branch_id: int = Field(default=0)
    metric_id: int = Field(default=0)
    record_date: date = Field(default_factory=date.today)
    value: float = Field(default=0.0)

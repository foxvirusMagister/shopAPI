import operator
from os import getenv
from fastapi import FastAPI, status, HTTPException
from sqlmodel import SQLModel, Field, create_engine, Session, select, text
from typing import Optional, List
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy.exc import IntegrityError, InternalError
from psycopg2.errors import RaiseException
from pydantic import ValidationError


load_dotenv()

app = FastAPI()

db_username = getenv("DB_USERNAME")
db_password = getenv("DB_PASSWORD")
db_url = getenv("DB_URL")
db_port = int(getenv("DB_PORT")) if getenv("DB_PORT") != "" else 5432
db_name = getenv("DB_NAME")

engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_url}:{db_port}/{db_name}")

class TerminalBase(SQLModel):
    name: str
    position: str
    place: str
    status: Optional[int] = 3
    total: Optional[float] = 0
    using_from: Optional[datetime]

class Terminal(TerminalBase, table=True):
    __tablename__ = "terminals"
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: Optional[datetime] = Field(default=None, sa_column_kwargs={"server_default": text("now()")})

class TerminalGet(TerminalBase):
    id: int
    status: int
    total: float
    created_at: datetime

class TerminalAdd(TerminalBase):
    pass

class TerminalSet(TerminalBase):
    name: Optional[str] = None
    position: Optional[str] = None
    place: Optional[str] = None
    status: Optional[int] = None
    total: Optional[float] = None
    using_from: Optional[datetime] = None

class GoodieBase(SQLModel):
    id: int
    name: str
    price: float
    amount: int
    description: Optional[str]

class Goodie(GoodieBase, table=True):
    __tablename__ = "goodies"
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: Optional[datetime] = Field(default=None, sa_column_kwargs={"server_default": text("now()")})

class GoodieGet(GoodieBase):
    created_at: datetime

class GoodieAdd(GoodieBase):
    pass

class GoodieSet(GoodieBase):
    id: Optional[int] = None
    name: Optional[str] = None
    price: Optional[float] = None
    amount: Optional[int] = None
    description: Optional[str] = None


class SellingBase(SQLModel):
    goodie_id: int
    terminal_id: int
    amount: int = Field(default=1, nullable=False)
    discount: Optional[float] = Field(default=0)
    selling_code: str

class Selling(SellingBase, table=True):
    __tablename__ = "sellings"
    id: int = Field(default=None, primary_key=True)
    total_price: Optional[float] = Field(default=None)
    created_at: Optional[datetime] = Field(default=None, sa_column_kwargs={"server_default": text("now()")})


class SellingGet(SellingBase):
    id: int
    total_price: float
    created_at: datetime

class SellingAdd(SellingBase):
    pass






@app.get("/terminals", response_model=List[TerminalGet])
def GetTerminals(filter: str | None = None, sort: str | None = None):
    with Session(engine) as session:
        filters = ["id", "name", "place", "position", "total", "using_from", "status"]
        data = session.exec(FilterAndSort(Terminal, filter, sort, filters)).all()
        return data
    
@app.get("/terminals/{id}", response_model=TerminalGet)
def GetTerminal(id: int):
    with Session(engine) as session:
        data = session.get(Terminal, id)
        if data:
            return data
        session.rollback()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Terminal {id} not found!")

@app.post("/terminals", response_model=TerminalGet)
def AddTerminal(value: TerminalAdd):
    with Session(engine) as session:
        data = Terminal(**value.model_dump())
        try:
            session.add(data)
            session.commit()
            session.refresh(data)
            return data
        except IntegrityError as e:
            raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail=f"We got an error, maybe some of your values are incorrect. Detail: {e}")
        finally:
            session.rollback()

@app.put("/terminals/{id}", response_model=TerminalGet)
def PutTerminal(id: int, value: TerminalSet):
    with Session(engine) as session:
        data = session.get(Terminal, id)
        if data:
            for item in value.model_dump():
                if eval(f"value.{item}") != None:
                    exec(f"data.{item} = value.{item}")
            session.add(data)
            session.commit()
            session.refresh(data)
            return data
        else:
            try:
                data = TerminalAdd(**value.model_dump())
                data = Terminal(**data.model_dump())
                session.add(data)
                session.commit()
                session.refresh(data)
                return data
            except ValidationError:
                raise HTTPException(detail="Required fields missing", status_code=status.HTTP_406_NOT_ACCEPTABLE)
            finally:
                session.rollback()
            
@app.delete("/terminals/{id}")
def DeleteTerminal(id: int):
    with Session(engine) as session:
        data = session.get(Terminal, id)
        if data:
            session.delete(data)
            session.commit()
            return id
        session.rollback()
        raise HTTPException(detail=f"Terminal with id {id} not found!", status_code=status.HTTP_404_NOT_FOUND)


@app.get("/goodies", response_model=List[GoodieGet])
def get_goodies(filter: str | None = None, sort: str | None = None):
    with Session(engine) as session:
        possible_filters = ["id", "name", "price", "amount", "description", "created_at"]
        data = session.exec(FilterAndSort(Goodie, filter, sort, possible_filters)).all()
        return data

@app.get("/goodies/{id}", response_model=GoodieGet)
def get_goodie(id: int):
    with Session(engine) as session:
        data = session.get(Goodie, id)
        if data:
            return data
        else:
            session.rollback()
            HTTPException(detail=f"Product {id} not found!", status_code=status.HTTP_404_NOT_FOUND)

@app.post("/goodies", response_model=GoodieGet)
def add_goodie(value: GoodieAdd):
    with Session(engine) as session:
        try:
            data = Terminal(**value.model_dump)
            session.add(data)
            session.commit()
            session.refresh(data)
            return data
        except IntegrityError as e:
            raise HTTPException(detail=f"We got an error, maybe some of yours field's values are incorrect. Error: {e}", status_code=status.HTTP_406_NOT_ACCEPTABLE)
        finally:
            session.rollback()

@app.put("/goodies/{id}", response_model=GoodieGet)
def set_goodie(id: int, value: GoodieSet):
    with Session(engine) as session:
        data = session.get(Goodie, id)
        if data:
            for _key, _value in value.model_dump().items():
                if _value != None:
                    setattr(data, _key, _value)
            session.add(data)
            session.commit()
            session.refresh(data)
            return data
        else:
            try:
                data = GoodieAdd(**value.model_dump())
                data = Goodie(**data.model_dump())
                session.add(data)
                session.commit()
                session.refresh(data)
                return data
            except ValidationError:
                raise HTTPException(detail="Required fields missing", status_code=status.HTTP_406_NOT_ACCEPTABLE)
            finally:
                session.rollback()

@app.delete("/goodies/{id}")
def delete_goodie(id: int):
    with Session(engine) as session:
        data = session.get(Goodie, id)
        if data:
            session.delete(data)
            session.commit()
            return 200
        session.rollback()
        raise HTTPException(detail=f"product {id} not found", status_code=status.HTTP_404_NOT_FOUND)


@app.get("/sellings", response_model=List[SellingGet])
def get_sellings(sort: str | None = None, filter: str | None = None):
    with Session(engine) as session:
        possible_filters: str = ["id", "goodie_id", "terminal_id", "amount", "total_price", "discount", "created_at", "selling_code"]
        data = session.exec(FilterAndSort(Selling, filter, sort, possible_filters)).all()
        return data

@app.get("/sellings/{id}", response_model=SellingGet)
def get_selling(id: int):
    with Session(engine) as session:
        data = session.get(Selling, id)
        if data:
            return data
        session.rollback()
        raise HTTPException(detail=f"Selling with id {id} was not found", status_code=status.HTTP_404_NOT_FOUND)
    
@app.post("/sellings", response_model=SellingGet)
def add_selling(new: SellingAdd):
    with Session(engine) as session:
        try:
            data = Selling(**new.model_dump())
            session.add(data)
            session.commit()
            session.refresh(data)
            return data
        except IntegrityError as e:
            raise HTTPException(detail=f"We got an error, maybe some of yours field's values are incorect! Error code: {e}", status_code=status.HTTP_406_NOT_ACCEPTABLE)
        except InternalError as e:
            raise HTTPException(detail=f"Price value is incorect: {e.orig}", status_code=status.HTTP_406_NOT_ACCEPTABLE)
        finally:
            session.rollback()
    
@app.delete("/sellings/{id}")
def delete_selling(id: int):
    with Session(engine) as session:
        data = session.get(Selling, id)
        if data:
            session.delete(data)
            return id
        session.rollback()
        raise HTTPException(detail=f"Selling with {id} was not found!", status_code=status.HTTP_404_NOT_FOUND)

def FilterAndSort(thing, filter, sort, possible_filters: List[str]):
    sign = "asc"
    sort_by = getattr(thing, possible_filters[0])
    result = select(thing)
    if filter != None:
        filter_args = filter.split(" ")
        if filter_args[0] in possible_filters:
            attribute = getattr(thing, filter_args[0])
            operation = getattr(operator, filter_args[1])
            value = filter_args[2]
            result = result.where(operation(attribute, value))
    if sort != None:
        if sort[0] == "-":
            sign = "desc"
            sort = sort[1:]
        elif sort[0] == "+":
            sort = sort[1:]
        if sort in possible_filters:
            sort_by = getattr(thing, sort)
        sort_by = getattr(sort_by, sign)
        result = result.order_by(sort_by())
    return result

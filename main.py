import operator
from os import getenv
from fastapi import FastAPI, status, HTTPException
from sqlmodel import SQLModel, Field, create_engine, Session, select, text
from typing import Optional, List
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy.exc import IntegrityError


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
    name: Optional[str]
    position: Optional[str]
    place: Optional[str]


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

@app.put("/terminals/{id}", response_model=TerminalGet)
def PutTerminal(id: int, value: TerminalSet):
    with Session(engine) as session:
        data = session.get(Terminal, id)
        if data:
            pass #Доделать
        

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

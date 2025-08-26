from typing import Any
from enum import Enum

from pydantic import BaseModel, Field


class DataSource(BaseModel):
    """ """

    __test__ = False

    id: int
    type: str
    params: dict[str, str | int | bool | None] | None = None
    extra: dict[str, str] | None = None


class DataSourceObject(BaseModel):
    """ """

    schema_name: str = Field(alias='schema')
    name: str
    type: str


class SimpleType(str, Enum):
    """
    Типы полей, которые исползуются в AW
    """

    string = 'string'
    number = 'number'
    float = 'float'
    date = 'date'
    bool = 'bool'


class ObjectColumnMeta(BaseModel):
    """ """

    name: str = Field(description='Название столбца')
    type: str = Field(description='Исходный тип столбца')
    simple_type: SimpleType = Field(description='Тип поля для AW')
    comment: str | None = Field(default=None, description='Комментарий к полю источника')


class ForeignKeyMeta(BaseModel):
    """ 
    Модель для описания связи к внешнему объекту источника
    """
    column_name: str
    foreign_table_schema: str
    foreign_table_name: str
    foreign_column_name: str


class ObjectMeta(BaseModel):
    """
    Метаданные объекта источника
    """

    columns: list[ObjectColumnMeta] = Field(
        description='Список столбцов модели',
        default=[],
    )
    foreign_keys: list[ForeignKeyMeta] = Field(
        description='Описание внешних связей объекта',
        default = []
    )


class ObjectData(BaseModel):
    """ """

    data: list[dict[str, str | int | float | bool | None]]


# ---------------------------------------------------------------------
# Описание тестовых случаев
# ---------------------------------------------------------------------
class TestCaseDataSourceObjectFilter(BaseModel):
    """ 
    """
    object_name: str
    field_name: str | None = None
    operator: str | None = None
    value: Any


class TestCaseDataSource(BaseModel):
    """
    Источник данных для тестового случая
    """

    id: int
    type: str
    params: dict[str, str | int | bool | None] | None = None
    extra: dict[str, str] | None = None

    objects: list[str] | str | None = None
    sql: list[str] | str | None = None
    filters: list[TestCaseDataSourceObjectFilter] | TestCaseDataSourceObjectFilter | None = None

    def to_data_source(self) -> DataSource:
        return DataSource(
            id=self.id,
            type=self.type,
            params=self.params,
            extra=self.extra,
        )

    def get_objects(self) -> list[str]:
        if self.objects is None:
            return []
        return self.objects if isinstance(self.objects, list) else [self.objects]
    
    def get_sql(self) -> list[str]:
        if self.sql is None:
            return []
        return self.sql if isinstance(self.sql, list) else [self.sql]
    
    def get_filters(self) -> list[TestCaseDataSourceObjectFilter]:
        if self.filters is None:
            return []
        return self.filters if isinstance(self.filters, list) else [self.filters]

class TestCaseDataSources(BaseModel):
    """ """

    __test__ = False

    available: TestCaseDataSource | list[TestCaseDataSource]
    unavailable: TestCaseDataSource | list[TestCaseDataSource]

    def available_data_sources(self) -> list[TestCaseDataSource]:
        return self.available if isinstance(self.available, list) else [self.available]

    def unavailable_data_sources(self) -> list[TestCaseDataSource]:
        return (
            self.unavailable
            if isinstance(self.unavailable, list)
            else [self.unavailable]
        )
    

class TestConnector(BaseModel):
    """ 
    Описание коннектора
    """
    url: str
    timeout: int | None = None


class TestS3Settings(BaseModel):
    """ 
    Описание подключения к S3 серверу
    """
    mock: bool
    endpoint_url: str
    username: str
    password: str
    bucket: str



class TestConfig(BaseModel):
    """
    Описание тестового случая
    """

    __test__ = False

    connector: TestConnector
    s3: TestS3Settings
    data_sources: TestCaseDataSources

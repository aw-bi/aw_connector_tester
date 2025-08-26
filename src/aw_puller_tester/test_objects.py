import uuid

import pytest
import httpx
from pydantic import ValidationError

from aw_puller_tester.dto import DataSourceObject
from aw_puller_tester.tools import assert_error_response


def test_objects_flat(test_config, connector_client):
    """
    Тест на получение списка объектов источника в проском виде
    """
    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={
                'data_source': data_source.model_dump(),
            },
        )

        assert r.status_code < 400, (
            f'HTTP {r.status_code} при получении списка объектов для источника id={data_source.id}'
        )

        try:
            objects = r.json()
        except Exception:
            pytest.fail(
                f'В ответе коннектора на получение списка объектов источника ожидался JSON-объект. Получено: {r.text}'
            )

        if not isinstance(objects, list):
            pytest.fail(
                f'В ответе коннектора на получение списка объектов источника ожидался список. Получено: {r.json()}'
            )

        for o in objects:
            try:
                DataSourceObject.model_validate(o)
            except ValidationError as e:
                pytest.fail(
                    f'Ошибка валидации объекта из ответа коннектора: {e.errors()}'
                )


def test_objects_non_flat(test_config, connector_client):
    """
    Тест на получение списка объектов источника в неплоском виде
    """
    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={'data_source': data_source.model_dump(), 'flat': False},
        )

        assert r.status_code < 400, (
            f'HTTP {r.status_code} при получении списка объектов id={data_source.id}'
        )

        try:
            objects = r.json()
        except Exception:
            pytest.fail(
                f'В ответе коннектора на получение списка объектов источника ожидался JSON-объект. Получено: {r.text}'
            )

        assert isinstance(objects, dict), (
            f'В ответе коннектора на получение списка объектов источника ожидался словарь. Получено: {r.json()}'
        )

        for schema, objects in objects.items():
            if not isinstance(schema, str):
                raise Exception(
                    f'В названиях схемы ожидался строковый тип. Получено: {schema} (тип {type(schema).__name__})'
                )

            if not isinstance(objects, list):
                raise Exception(
                    f'В списке объектов схемы {schema} ожидался список объектов. Получено: {objects} (тип {type(objects).__name__})'
                )

            for o in objects:
                if not isinstance(o, str):
                    raise Exception(
                        f'В списке объектов схемы {schema} ожидались строковые названия объектов. Получено: {o} (тип {type(o).__name__})'
                    )


def test_objects_query_string_flat(test_config, connector_client):
    """
    Проверяется формат ответа на получение списка объектов с фильтром
    """

    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        if not available_data_source.get_objects():
            # В описании тестового случая нет ни одного доступного объекта. Данный источник пропускается
            continue

        tested = True

        query_string = available_data_source.get_objects()[0].split('.')[-1]

        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={
                'data_source': data_source.model_dump(),
                'query_string': query_string,
            },
        )

        assert r.status_code < 400, (
            f'HTTP {r.status_code} при получении списка объектов для источника id={data_source.id} '
            f'с условием query_string={query_string}'
        )

        try:
            objects = r.json()
        except Exception:
            pytest.fail(
                f'В ответе коннектора на получение списка объектов источника ожидался JSON-объект. Получено: {r.text}'
            )

        assert isinstance(objects, list), (
            f'В ответе коннектора на получение списка объектов источника ожидался список. Получено: {r.json()}'
        )

        assert len(objects) > 0, (
            f'В списке объектов источника id={data_source.id} по запросу query_string="{query_string}" не найден ни один объект'
        )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_objects_query_string_flat_not_found(test_config, connector_client):
    """
    Тест на получение списка объектов с фильтром, под который не подходит ни один объект
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        if not available_data_source.get_objects():
            # В описании тестового случая нет ни одного доступного объекта. Данный источник пропускается
            continue

        tested = True

        query_string = uuid.uuid4().hex

        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={
                'data_source': data_source.model_dump(),
                'query_string': query_string,
            },
        )

        assert r.status_code < 400, (
            f'HTTP {r.status_code} при получении списка объектов для источника id={data_source.id} '
            f'с условием query_string={query_string}'
        )

        try:
            objects = r.json()
        except Exception:
            pytest.fail(
                f'В ответе коннектора на получение списка объектов источника ожидался JSON-объект. Получено: {r.text}'
            )

        assert isinstance(objects, list), (
            f'В ответе коннектора на получение списка объектов источника ожидался список. Получено: {r.json()}'
        )

        assert len(objects) == 0, (
            f'В списке объектов источника id={data_source.id} с фильтром по случайной строке были найдены объекты'
        )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_objects_query_string_non_flat(test_config, connector_client):
    """
    Проверяется формат ответа на получение списка объектов с фильтром
    """

    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        if not available_data_source.get_objects():
            # В описании тестового случая нет ни одного доступного объекта. Данный источник пропускается
            continue

        tested = True

        query_string = available_data_source.get_objects()[0].split('.')[-1]

        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={
                'data_source': data_source.model_dump(),
                'query_string': query_string,
                'flat': False,
            },
        )

        assert r.status_code < 400, (
            f'HTTP {r.status_code} при получении списка объектов для источника id {data_source.id} '
            f'с условием query_string={query_string}'
        )

        try:
            objects = r.json()
        except Exception:
            pytest.fail(
                'В ответе коннектора на получение списка объектов источника ожидался JSON-объект. '
                f'Получено: {r.text}'
            )

        assert isinstance(objects, dict), (
            'В ответе коннектора на получение списка объектов источника ожидался словарь. '
            f'Получено: {r.json()}'
        )

        assert len(objects) > 0, (
            f'В списке объектов источника id={data_source.id} по запросу query_string="{query_string}" '
            'не найден ни один объект'
        )

        for schema_name, tables in objects.items():
            assert len(tables) > 0, (
                f'В списке объектов источника id={data_source.id} по запросу query_string="{query_string}" '
                f'для схемы {schema_name} указан пустой список таблиц'
            )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_objects_query_string_non_flat_not_found(test_config, connector_client):
    """
    Тест на получение списка объектов с фильтром, под который не подходит ни один объект
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        if not available_data_source.get_objects():
            # В описании тестового случая нет ни одного доступного объекта. Данный источник пропускается
            continue

        tested = True

        query_string = uuid.uuid4().hex
        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={
                'data_source': data_source.model_dump(),
                'query_string': query_string,
                'flat': False,
            },
        )

        assert r.status_code < 400, (
            f'HTTP {r.status_code} при получении списка объектов для источника id {data_source.id} '
            f'с условием query_string={query_string}'
        )

        try:
            objects = r.json()
        except Exception:
            pytest.fail(
                f'В ответе коннектора на получение списка объектов источника ожидался JSON-объект. Получено: {r.text}'
            )

        assert isinstance(objects, dict), (
            f'В ответе коннектора на получение списка объектов источника ожидался словарь. Получено: {r.json()}'
        )

        assert len(objects) == 0, (
            f'В списке объектов источника id={data_source.id} с фильтром по случайной строке были найдены объекты'
        )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_objects_unavailable(test_config, connector_client):
    """
    Проверяется формат ответа на получение списка объектов для недоступного источника
    """
    for unavailable_data_source in test_config.data_sources.unavailable_data_sources():
        data_source = unavailable_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/objects',
            json={
                'data_source': data_source.model_dump(),
            },
        )

        assert_error_response(r)

import pytest
import httpx
import uuid
from pydantic import ValidationError

from aw_puller_tester.dto import ObjectMeta
from aw_puller_tester.tools import assert_error_response


def test_object_meta(test_config, connector_client):
    """
    Тест на получение метаданных объекта
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        for object_name in available_data_source.get_objects():
            tested = True

            data_source = available_data_source.to_data_source()

            r = connector_client.post(
                url='data-source/object-meta',
                json={
                    'data_source': data_source.model_dump(),
                    'object_name': object_name,
                },
            )

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при получении метаданных объекта {object_name}: {r.text}'
            )

            try:
                response_json = r.json()
            except Exception:
                pytest.fail(
                    f'В ответе коннектора на получение метаданных объекта {object_name} ожидался JSON-объект. Получено: {r.text}'
                )

            try:
                object_meta = ObjectMeta.model_validate(response_json)
            except ValidationError as e:
                pytest.fail(
                    f'Ошибка валидации метаданных объекта из ответа коннектора: {e.errors()}'
                )

            assert len(object_meta.columns) > 0, (
                f'В ответе коннектора нет столбцов для объекта {object_name}'
            )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_missing_object_meta(test_config, connector_client):
    """
    Тест на попытку получения метаданных для отсутствующих объектов источника
    """
    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()
        # случайная строка, которая не должна встретиться в названиях объектов источника
        object_name = uuid.uuid4().hex

        r = connector_client.post(
            'data-source/object-meta',
            json={
                'data_source': data_source.model_dump(),
                'object_name': object_name,
            },
        )

        assert_error_response(r)


def test_object_meta_invalid_request(test_config, connector_client):
    """
    Тест на невалидные запросы, отправляемые в источник
    """

    data_source = test_config.data_sources.available_data_sources()[0].to_data_source()

    BAD_REQUESTS = [
        {},
        {'data_source': data_source.model_dump()},
    ]

    if test_config.data_sources.available_data_sources()[0].get_objects():
        object_name = test_config.data_sources.available_data_sources()[
            0
        ].get_objects()[0]

        BAD_REQUESTS.extend(
            [
                {'object_name': object_name},
                {'data_source': data_source.model_dump(), 'object_name': 1},
                {'data_source': 1, 'object_name': object_name},
            ]
        )

    for bad_request in BAD_REQUESTS:
        r = connector_client.post(url='data-source/object-meta', json=bad_request)

        assert_error_response(r, request_data=bad_request)


def test_object_meta_unavailable_data_source(test_config, connector_client):
    """
    Тест на запрос к коннектору с несуществующим источником данных
    """
    for unavailable_data_source in test_config.data_sources.unavailable_data_sources():
        data_source = unavailable_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/object-meta',
            json={
                'data_source': data_source.model_dump(),
                'object_name': uuid.uuid4().hex,
            },
        )

        assert_error_response(r)

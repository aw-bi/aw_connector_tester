import uuid

import pytest
from pydantic import ValidationError

from aw_puller_tester.dto import ObjectData
from aw_puller_tester.tools import assert_error_response


def test_object_data(test_config, connector_client):
    """
    Проверка получения данных объекта из источника
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        for object_name in available_data_source.get_objects():
            tested = True
            data_source = available_data_source.to_data_source()

            r = connector_client.post(
                url='data-source/object-data',
                json={
                    'data_source': data_source.model_dump(),
                    'object_name': object_name,
                },
            )

            assert r.status_code < 400, (
                f'Ожидался ответ коннектора с ошибочным HTTP статусом. Получено HTTP {r.status_code}: {r.text}.'
            )

            try:
                response_json = r.json()
            except Exception:
                pytest.fail(
                    f'В ответе коннектора ожидался JSON-объект. Получено: {r.text}'
                )

            try:
                object_data = ObjectData.model_validate(response_json)
            except ValidationError as e:
                pytest.fail(f'Ошибка валидации ответа коннектора: {e}')

            assert len(object_data.data) > 0, (
                f'В ответе коннектора нет данных для объекта {object_name}'
            )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_missing_object_data(test_config, connector_client):
    """
    Тест на попытку получения данных для отсутствующего объекта источника
    """
    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()
        # случайная строка, которая не должна встретиться в названиях объектов источника
        object_name = uuid.uuid4().hex

        r = connector_client.post(
            'data-source/object-data',
            json={
                'data_source': data_source.model_dump(),
                'object_name': object_name,
            },
        )

        assert_error_response(r)


def test_object_data_invalid_request(test_config, connector_client):
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
        r = connector_client.post(url='data-source/object-data', json=bad_request)

        assert_error_response(r, request_data=bad_request)


def test_object_data_unavailable_data_source(test_config, connector_client):
    """
    Тест на запрос к коннектору с несуществующим источником данных
    """
    for unavailable_data_source in test_config.data_sources.unavailable_data_sources():
        data_source = unavailable_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/object-data',
            json={
                'data_source': data_source.model_dump(),
                'object_name': uuid.uuid4().hex,
            },
        )

        assert_error_response(r)

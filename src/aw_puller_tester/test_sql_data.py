import uuid

import pytest
import httpx
from pydantic import ValidationError

from aw_puller_tester.dto import ObjectData
from aw_puller_tester.tools import assert_error_response


def test_sql_data(test_config, connector_client):
    """
    Проверка получения данных объекта из источника
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for sql_text in available_data_source.get_sql():
            tested = True
            

            r = connector_client.post(
                url='data-source/sql-object-data',
                json={
                    'data_source': data_source.model_dump(),
                    'sql_text': sql_text,
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
                f'В ответе коннектора нет данных для sql-запроса {sql_text}'
            )

    if not tested:
        pytest.skip(
            'Нет доступных SQL запросов для тестирования'
        )
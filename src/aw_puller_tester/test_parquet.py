import os
import uuid

import pytest

from aw_puller_tester.dto import ObjectMeta
from aw_puller_tester.tools import (
    assert_error_response,
    read_and_assert_exported_parquet,
    request_parquet_and_wait,
    delete_s3_folder,
)


def test_parquet(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """
    Проверка выгрузки в parquet для объекта источника
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for object_name in available_data_source.get_objects():
            # очищаем папку запуска
            delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

            tested = True
            export_path_key = os.path.join(etl_temp_run_folder, 'data.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {object_name}: {r.text}'
            )

            read_and_assert_exported_parquet(
                s3_client=etl_s3_client,
                bucket=etl_s3_bucket,
                exported_parquet_key=export_path_key,
            )

    if not tested:
        pytest.skip('Нет доступных объектов для тестирования')


def test_parquet_with_fields(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """
    Проверка выгрузки паркета по специальному списку полей
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for object_name in available_data_source.get_objects():
            # очищаем папку запуска
            delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

            tested = True

            # 1. запрашиваем список столбцов объекта
            r = connector_client.post(
                url='data-source/object-meta',
                json={
                    'data_source': data_source.model_dump(),
                    'object_name': object_name,
                },
            )

            assert r.status_code < 400, (
                f'Не удалось получить метаданные объекта {object_name} источника {data_source.id}'
            )

            object_meta = ObjectMeta.model_validate(r.json())

            # 2. запрашиваем выгрузку в parquet без указания списка столбцов
            export_path_key = os.path.join(
                etl_temp_run_folder, 'data_all_columns.parquet'
            )

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {object_name}: {r.text}'
            )

            table_all_columns = read_and_assert_exported_parquet(
                s3_client=etl_s3_client,
                bucket=etl_s3_bucket,
                exported_parquet_key=export_path_key,
            )

            for column in object_meta.columns:
                assert column.name in table_all_columns.column_names, (
                    f'Столбец {column.name} не найден в выгруженных данных объекта {object_name} '
                    f'источника id={data_source.id}'
                )

            # 3. Запрашиваем выгрузку в parquet только первого столбца

            first_column = object_meta.columns[0]

            # для тестов выгрузки в parquet берем только одно первое поле
            parquet_fields = [
                {'name': first_column.name, 'type': first_column.simple_type}
            ]
            export_path_key = os.path.join(etl_temp_run_folder, 'data.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                        'fields': parquet_fields,
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {object_name}: {r.text}'
            )

            table_one_column = read_and_assert_exported_parquet(
                s3_client=etl_s3_client,
                bucket=etl_s3_bucket,
                exported_parquet_key=export_path_key,
            )

            assert table_one_column.column_names == [first_column.name], (
                f'В данных объекта {object_name} источника id={data_source.id} есть лишние столбцы'
            )

    if not tested:
        pytest.skip('Не доступных объектов для тестирования')


def test_parquet_with_limit(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """
    Проверяет, как выгружаются данные в parquet с limit
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for object_name in available_data_source.get_objects():
            # очищаем папку запуска
            delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

            # 1. Сначала делаем запрос за всеми данными
            export_path_key = os.path.join(etl_temp_run_folder, 'data_all.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {object_name}: {r.text}'
            )

            table_all = read_and_assert_exported_parquet(
                s3_client=etl_s3_client,
                bucket=etl_s3_bucket,
                exported_parquet_key=export_path_key,
            )

            if table_all.num_rows < 2:
                # для данного объекта запускать LIMIT 1 нет смысла, пропускаем его
                continue

            tested = True
            # 2. Делаем запрос с LIMIT 1
            export_path_key = os.path.join(etl_temp_run_folder, 'data_limit.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'limit': 1,
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {object_name}: {r.text}'
            )

            table = read_and_assert_exported_parquet(
                etl_s3_client, etl_s3_bucket, export_path_key
            )

            assert table.num_rows == 1, f'LIMIT 1 не применился объекта {object_name}'

    if not tested:
        pytest.skip('Нет доступных фильтров для тестирования')


def test_parquet_with_filters(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """
    Проверка выгрузки данных в parquet с дополнительными условиями
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for filter in available_data_source.get_filters():
            # очищаем папку запуска
            delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

            tested = True

            # 1. сначала сделаем запрос за всеми данными
            export_path_key = os.path.join(etl_temp_run_folder, 'data_all.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': filter.object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {filter.object_name} для источника id={data_source.id}: {r.text}'
            )

            table_all = read_and_assert_exported_parquet(
                etl_s3_client, etl_s3_bucket, export_path_key
            )

            # 2. Делаем запрос из-под фильтра
            export_path_key = os.path.join(etl_temp_run_folder, 'data_filtered.parquet')
            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': filter.object_name,
                        'type': 'table',
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [
                        {
                            'field_name': filter.field_name,
                            'operator': filter.operator,
                            'value': filter.value,
                        }
                    ],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных объекта {filter.object_name} для источника id={data_source.id}: {r.text}'
            )

            table_filter = read_and_assert_exported_parquet(
                etl_s3_client, etl_s3_bucket, export_path_key
            )

            # Под фильтром должно получиться меньше записей
            assert table_filter.num_rows < table_all.num_rows, (
                f'Фильтр {filter} не применился для источника id={data_source.id}'
            )

    if not tested:
        pytest.skip('Нет доступных фильтров для тестирования')


def test_parquet_sql(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """
    Проверка выгрузки в parquet для SQL-запроса
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for sql_text in available_data_source.get_sql():
            # очищаем папку запуска
            delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

            tested = True

            export_path_key = os.path.join(etl_temp_run_folder, 'data.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': 'jsql',
                        'type': 'sql',
                        'query_text': sql_text,
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных для SQL запроса {sql_text}: {r.text}'
            )

            read_and_assert_exported_parquet(
                etl_s3_client, etl_s3_bucket, export_path_key
            )

    if not tested:
        pytest.skip('Нет доступных SQL запросов для тестирования')


def test_parquet_sql_with_limit(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """
    Проверяет как выполняется выгрузка в parquet с указанием limit
    """
    tested = False
    for available_data_source in test_config.data_sources.available_data_sources():
        data_source = available_data_source.to_data_source()

        for sql_text in available_data_source.get_sql():
            # очищаем папку запуска
            delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

            tested = True

            # 1. Сначала делаем запрос за всеми данными
            export_path_key = os.path.join(etl_temp_run_folder, 'data_all.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': 'jsql',
                        'type': 'sql',
                        'query_text': sql_text,
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'filters': [],
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных SQL запроса {sql_text}: {r.text}'
            )

            table_all = read_and_assert_exported_parquet(
                s3_client=etl_s3_client,
                bucket=etl_s3_bucket,
                exported_parquet_key=export_path_key,
            )

            if table_all.num_rows < 2:
                # для данного объекта запускать LIMIT 1 нет смысла, пропускаем его
                continue

            tested = True
            # 2. Делаем запрос с LIMIT 1
            export_path_key = os.path.join(etl_temp_run_folder, 'data_limit.parquet')

            r = request_parquet_and_wait(
                client=connector_client,
                request_json={
                    'object': {
                        'name': 'jsql',
                        'type': 'sql',
                        'query_text': sql_text,
                        'data_source': data_source.model_dump(),
                    },
                    'folder': f's3://{export_path_key}',
                    'limit': 1,
                },
            )

            assert r is not None, 'Объект ответа не должен быть пустым'

            assert r.status_code < 400, (
                f'HTTP {r.status_code} при выгрузке данных SQL запроса {sql_text}: {r.text}'
            )

            table = read_and_assert_exported_parquet(
                etl_s3_client, etl_s3_bucket, export_path_key
            )

            assert table.num_rows == 1, (
                f'LIMIT 1 не применился для SQL запроса {sql_text}'
            )

    if not tested:
        pytest.skip('Нет доступных SQL запросов для тестирования')


def test_missing_object_parquet(
    test_config, connector_client, etl_s3_client, etl_s3_bucket, etl_temp_run_folder
):
    """ """

    for available_data_source in test_config.data_sources.available_data_sources():
        # очищаем папку запуска
        delete_s3_folder(etl_s3_client, etl_s3_bucket, etl_temp_run_folder)

        data_source = available_data_source.to_data_source()
        # случайная строка, которая не должна встретиться в названиях объектов источника
        object_name = uuid.uuid4().hex

        folder = os.path.join(etl_temp_run_folder, 'data.parquet')

        r = request_parquet_and_wait(
            client=connector_client,
            request_json={
                'object': {
                    'name': object_name,
                    'data_source': data_source.model_dump(),
                },
                'folder': f's3://{folder}',
                'filters': [],
            },
        )

        assert_error_response(r)


def test_parquet_invalid_request(test_config, connector_client):
    """ """
    data_source = test_config.data_sources.available_data_sources()[0].to_data_source()

    BAD_REQUESTS = [
        {},
        {'object': {'data_source': data_source.model_dump()}},
    ]

    for bad_request in BAD_REQUESTS:
        r = connector_client.post(url='data-source/parquet', json=bad_request)

        assert_error_response(r, request_data=bad_request)


def test_parquet_unavailable_data_source(
    test_config, connector_client, etl_temp_run_folder
):
    """
    Тест на запрос к коннектору с несуществующим источником данных
    """
    for unavailable_data_source in test_config.data_sources.unavailable_data_sources():
        data_source = unavailable_data_source.to_data_source()

        folder = os.path.join(etl_temp_run_folder, 'data.parquet')

        r = connector_client.post(
            url='data-source/parquet',
            json={
                'object': {
                    'name': uuid.uuid4().hex,
                    'data_source': data_source.model_dump(),
                },
                'folder': f's3://{folder}',
                'filters': [],
            },
        )

        assert_error_response(r)

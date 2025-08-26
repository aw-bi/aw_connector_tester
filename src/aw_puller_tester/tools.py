import os
import json
import time
import tempfile
from pathlib import Path

import pytest
import httpx
import pyarrow
import pyarrow.parquet
from botocore.client import BaseClient
from botocore.exceptions import ClientError


def assert_error_response(r: httpx.Response | None, request_data: dict | None = None):
    """
    Проверяет, что ответ на http запрос содержит ошибку
    """
    assert r is not None, 'Объект ответа не должен быть пустым'

    assert r.status_code >= 400, (
        f'Ожидался ответ коннектора с ошибочным HTTP статусом. Получено HTTP {r.status_code}: {r.text}.'
        + (
            f' Параметры запроса: {json.dumps(request_data, ensure_ascii=False)}'
            if request_data is not None
            else ''
        )
    )

    try:
        error = r.json()
    except Exception:
        pytest.fail(f'В ответе коннектора ожидался JSON-объект. Получено: {r.text}')

    assert isinstance(error, dict), (
        f'В ответе коннектора ожидался словарь. Получено: {r.json()}'
    )

    assert 'detail' in error, (
        f'В ответе коннектора ожидалось сообщение об ошибке (параметр detail: "..."). Получено: {r.json()}'
    )


def request_parquet_and_wait(
    client: httpx.Client, request_json: dict
) -> httpx.Response | None:
    """
    Вспомогательная функция, которая запрашивает выгрузку в паркет и ожидает завершения.
    """
    wait = True
    check_location = None
    check_retry_after = 5  # количество секунд, через которое нужно повторить запрос
    r = None

    while wait:
        if check_location is None:
            # первый запрос
            r = client.post(url='data-source/parquet', json=request_json)
        else:
            time.sleep(check_retry_after)  # ждем перед следующим запросом
            r = client.get(url=check_location)

        if r.status_code == 202:
            # сервер ответил, что надо продолжать ждать
            assert 'Location' in r.headers, 'В ответе должен быть заголовок Location'

            check_location = r.headers['Location']
            if 'Retry-After' in r.headers:
                try:
                    check_retry_after = float(r.headers['Retry-After'])
                except ValueError:
                    pytest.fail('В заголовоке Retry-After нужно указать число')

                if check_retry_after <= 0:
                    pytest.fail(
                        'В заголовоке Retry-After нужно указать положительное число'
                    )
        else:
            wait = False
            # если ответ не 202, то это конечный ответ

    return r


def read_parquet_table(s3_client: BaseClient, bucket: str, key: str) -> pyarrow.Table:
    """
    Считывает данные из parquet-файла по указанному ключу в S3 хранилище
    """
    key = key.strip()
    filename = Path(key).name
    
    is_folder = key.endswith('/')
    if not is_folder:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                is_folder = True
            else:
                raise

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)
        
        if is_folder:
            folder_key = f'{key}/' if key[-1:] != '/' else key

            paginator = s3_client.get_paginator('list_objects')
            page_iterator = paginator.paginate(
                Bucket=bucket, Prefix=folder_key, Delimiter='/', PaginationConfig={'PageSize': 100}
            )

            os.makedirs(tempdir / filename)
            for page in page_iterator:
                for o in (page.get('Contents') or []):
                    partname = Path(o['Key']).name
                    obj = s3_client.get_object(Bucket=bucket, Key=o['Key'])
                    with open(tempdir / filename / partname, 'wb') as f:
                        f.write(obj['Body'].read())
        else:
            with open(tempdir / filename, 'wb') as f:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                f.write(obj['Body'].read())

        return pyarrow.parquet.read_table(tempdir / filename)
    

def read_and_assert_exported_parquet(s3_client: BaseClient, bucket: str, exported_parquet_key: str) -> pyarrow.Table:
    """ 
    Проверяет выгруженный коннектором parquet файл и возвращает его данные в виде pyarrow таблицы
    """
    exported_parquet_key = exported_parquet_key.strip().rstrip('/') + '/'

    # 1. Проверяем, что выгруженные данные есть в S3
    result = s3_client.list_objects(
        Bucket=bucket, Prefix=f'{exported_parquet_key}', MaxKeys=1
    )

    assert 'Contents' in result and len(result['Contents']) > 0, (
        f'В S3 нет файлов в папке {exported_parquet_key}'
    )

    # 2. Читаем данные
    table = read_parquet_table(s3_client, bucket, exported_parquet_key)

    assert table.num_rows > 0, f'В таблице {exported_parquet_key} нет строк'

    return table


def delete_s3_folder(s3_client: BaseClient, bucket: str, folder_key: str):
    """ 
    Удаляет папку в S3
    """
    folder_key = folder_key.strip().rstrip('/') + '/'

    paginator = s3_client.get_paginator('list_objects')
    page_iterator = paginator.paginate(
        Bucket=bucket, Prefix=folder_key, PaginationConfig={'PageSize': 100}
    )

    for page in page_iterator:
        if page.get('Contents'):
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': [{'Key': obj['Key']} for obj in page.get('Contents')], 'Quiet': True},
            )
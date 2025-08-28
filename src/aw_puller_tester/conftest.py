from typing import Generator
import uuid
from pathlib import Path
from urllib.parse import urlparse

import yaml
import pytest
import httpx
from pydantic import ValidationError
from moto.server import ThreadedMotoServer
import boto3

from aw_puller_tester.dto import TestConfig
from aw_puller_tester.tools import delete_s3_folder


@pytest.fixture(scope='session')
def test_config():
    """
    Фикступа с объектом конфигурации тестовых случаев
    """
    test_config: TestConfig | None = None

    for folder in Path(__file__).parents:
        test_config_file = folder / 'test_config.yml'
        if test_config_file.exists():
            with open(test_config_file, 'r') as f:
                try:
                    return TestConfig.model_validate(yaml.safe_load(f))
                except ValidationError as e:
                    pytest.exit(f'Конфигурация тестов из {test_config_file} некорректна: {e}')
        
    if test_config is None:
        pytest.exit('Файл с конфигурацией тестов test_config.yml не найден')

    return test_config


@pytest.fixture(scope='session')
def connector_url(test_config) -> str:
    """
    Возвращает строку с URL к тестируемому коннектору
    """
    return test_config.connector.url.strip().rstrip('/') + '/'


@pytest.fixture(scope='session')
def connector_timeout(test_config) -> int:
    """
    Возвращает таймаут запроса к коннектору
    """
    return test_config.connector.timeout or 10


@pytest.fixture(scope='function')
def connector_client(
    connector_url: str, connector_timeout: int
) -> Generator[httpx.Client, None, None]:
    with httpx.Client(base_url=connector_url, timeout=connector_timeout) as client:
        yield client


@pytest.fixture(scope='session')
def etl_s3_client(test_config):
    """ 
    Возвращает клиент к S3 серверу для ETL процессов
    """
    parsed_url = urlparse(test_config.s3.endpoint_url)

    if test_config.s3.mock:
        moto_server = ThreadedMotoServer(ip_address=parsed_url.hostname, port=parsed_url.port)
        moto_server.start()

    s3_client = boto3.client(
        's3',
        endpoint_url=test_config.s3.endpoint_url,
        aws_access_key_id=test_config.s3.username,
        aws_secret_access_key=test_config.s3.password,
    )

    if test_config.s3.mock:
        s3_client.create_bucket(Bucket='aw-etl')

    try:
        yield s3_client
    finally:
        s3_client.close()

        if test_config.s3.mock:
            # чистим за собой mock сервер
            httpx.post(f'http://{parsed_url.netloc}/moto-api/reset')
            moto_server.stop()


@pytest.fixture(scope='session')
def etl_s3_bucket(test_config):
    return test_config.s3.bucket


@pytest.fixture(scope='function')
def etl_temp_run_folder(etl_s3_client, etl_s3_bucket):
    """ 
    Возвращает временную etl папку в runs/. После выполнения теста содержимое папки очищается 
    """
    folder_key = f'runs/{uuid.uuid4().hex}'

    try:
        yield folder_key
    finally:
        delete_s3_folder(
            s3_client=etl_s3_client,
            bucket=etl_s3_bucket,
            folder_key=folder_key
        )
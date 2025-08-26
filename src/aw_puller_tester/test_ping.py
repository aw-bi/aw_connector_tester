import pytest

from aw_puller_tester.tools import assert_error_response



def test_ping(test_config, connector_client):
    """ 
    Проверка работоспособности источника
    """
    tested = False

    for available_data_source in test_config.data_sources.available_data_sources():
        tested = True
        data_source = available_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/ping',
            json=data_source.model_dump())
        
        assert r.status_code < 400, (
            f'HTTP {r.status_code} при пинге источника id={data_source.id}: {r.text}'
        )

    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )


def test_ping_unavailable(test_config, connector_client):
    """ 
    Проверка работоспособности недоступного источника
    """
    tested = False
    for unavailable_data_source in test_config.data_sources.unavailable_data_sources():
        tested = True

        data_source = unavailable_data_source.to_data_source()

        r = connector_client.post(
            url='data-source/ping',
            json=data_source.model_dump(),
        )

        assert_error_response(r)


    if not tested:
        pytest.skip(
            'Нет доступных объектов для тестирования'
        )
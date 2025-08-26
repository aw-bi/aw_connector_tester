def test_health(connector_client, etl_s3_client):
    """ 
    Проверка метода /health
    """
    r = connector_client.get('/health')

    assert r.status_code < 400, (
        f'Ошибка запроса GET /health: HTTP {r.status_code} {r.text}'
    )
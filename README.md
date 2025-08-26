# Тестер коннекторов для AW BI

## Установка Docker

Установите [Docker](https://docs.docker.com/engine/install/). Для Windows можно воспользоваться:
* [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/)
* установкой Docker в WSL [Установка WSL (Windows Subsystem for Linux) | Microsoft Learn](https://learn.microsoft.com/ru-ru/windows/wsl/install) + [Install Docker Engine on Ubuntu | Docker Documentation](https://docs.docker.com/engine/install/ubuntu/).

## Конфигурация тестов

Конфигурация запуска тестов задается в файле test_config.yml. Внутри конфигурации указываются:

* Параметры подключения к коннектору;
* Параметры подключения к S3 серверу (или запуска временного S3 сервера);
* Описание тестовых источников, данные которых используются при запуске тестов. Здесь указывается один или несколько источников, которые доступны для запросов к коннектору.
А также, указывается один несуществующий (недоступный) источник - на нем проверяются ошибки, которые возвращаются коннектором.

Подключение к коннектору:
<table>
  <tr>
    <th>Параметр</th>
    <th>Тип</th>
    <th>Обязательно</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td><nobr>connector.url</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>URL к API тестируемого коннектора. Например, http://192.168.1.9:9911</td>
  </tr>
  <tr>
    <td><nobr>connector.timeout</nobr></td>
    <td>integer</td>
    <td>нет</td>
    <td>Таймаут для выполнения HTTP запросов к коннектору. По умолчанию, 10.</td>
  </tr>
</table>

Подключение к S3 серверу:
<table>
  <tr>
    <th>Параметр</th>
    <th>Тип</th>
    <th>Обязательно</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td><nobr>s3.mock</nobr></td>
    <td>boolean</td>
    <td>да</td>
    <td>
        Если true, то при запуске тестов нужено поднять временный S3 сервер. Иначе, 
        подключаться к уже существующему серверу
    </td>
  </tr>
  <tr>
    <td><nobr>s3.endpoint_url</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>
    URL для обращения к API S3 сервера. 
    <br><br>       
    Если mock: true, то в этом URL нужно указать IP адрес и порт, на котором будет поднят
    временный S3 сервер. Например, http://0.0.0.0:9000
    </td>
  </tr>
  <tr>
    <td><nobr>s3.username</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>Имя пользователя (access_key) для подключения к S3 серверу</td>
  </tr>
  <tr>
    <td><nobr>s3.password</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>Пароль (secret_key) для подключения к S3 серверу</td>
  </tr>
  <tr>
    <td><nobr>s3.bucket</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>Название S3 бакета, который будет использоваться для выгрузки parquet файлов с данными источника</td>
  </tr>
</table>

Список доступных для тестов источников перечисляется в data_source.available. Эти источники используются для позитивных тестов коннектора.

<table>
  <tr>
    <th>Параметр</th>
    <th>Тип</th>
    <th>Обязательно</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].id</nobr></td>
    <td>integer</td>
    <td>да</td>
    <td>Идентификатор источника</td>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].type</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>Тип источника</td>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].params</nobr></td>
    <td>object</td>
    <td>да</td>
    <td>Параметры подключения к источнику (host, port, db, username, password)</td>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].extra</nobr></td>
    <td>object</td>
    <td>нет</td>
    <td>Дополнительные параметры источника</td>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].objects</nobr></td>
    <td>list of string</td>
    <td>да</td>
    <td>Список названий объектов, доступных в источнике. Названия указываются вместе со схемой.
    <br><br>
    Например,
    <pre>
    objects:
      - public.table1
      - public.table2
    </pre>
    </td>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].sql</nobr></td>
    <td>list of string</td>
    <td>нет</td>
    <td>Список SQL запросов, которые можно отсылать в источник. Если источник не поддерживает выполнение SQL запросов, то здесь можно ничего не указывать, и соответствующие тесты будут пропущены.
    </td>
  </tr>
  <tr>
    <td><nobr>data_source.available[i].filters</nobr></td>
    <td>list of objects</td>
    <td>нет</td>
    <td>Список фильтров, которые передаются в запрос на выгрузку данных в parquet.
    Здесь рекомендуется указать как минимум один фильтр в полной форме (object_name, field_name, operator, value) и как минимум один фильтр в укороченной форме (object_name, value).
    </td>
  </tr>
</table>

Один недоступный источник указывается data_source.unavailable. Любые запросы к коннектору с такими параметрами должны возвращать ошибки.

<table>
  <tr>
    <th>Параметр</th>
    <th>Тип</th>
    <th>Обязательно</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td><nobr>data_source.unavailable.id</nobr></td>
    <td>integer</td>
    <td>да</td>
    <td>Идентификатор источника</td>
  </tr>
  <tr>
    <td><nobr>data_source.unavailable.type</nobr></td>
    <td>string</td>
    <td>да</td>
    <td>Тип источника</td>
  </tr>
  <tr>
    <td><nobr>data_source.unavailable.params</nobr></td>
    <td>object</td>
    <td>да</td>
    <td>Параметры подключения к источнику (host, port, db, username, password)</td>
  </tr>
  <tr>
    <td><nobr>data_source.unavailable.extra</nobr></td>
    <td>object</td>
    <td>нет</td>
    <td>Дополнительные параметры источника</td>
  </tr>
</table>

## Пример конфигурации тестов

Приведем конфигурацию для тестирования [примера кастомного коннектора](https://github.com/aw-bi/aw_connector_example).

В .env файле этого коннектора указано:

```sh
CONNECTOR_PORT=192.168.1.9:9911
ETL_S3_URL=http://test:test@192.168.1.9:9000
ETL_S3_BUCKET=aw-etl
```

```yml
connector:
  url: http://192.168.1.9:9911
  timeout: 10

s3:
  mock: true
  endpoint_url: http://192.168.1.9:9000
  username: test
  password: test
  bucket: aw-etl

data_sources:
  available:
    - id: 1
      type: my-platform
      params:
        db: db1
      extra:
      objects:
        # Доступные в источнике объекты в формате schema_name.object_name
        - public.table1
        - work.table4
      sql: 
        - select id from table2
      filters:
        - object_name: public.table1
          field_name: id
          operator: ">"
          value: 1

        - object_name: public.table1
          field_name: name
          operator: IN
          value: "('name 1', 'name 2')"

        - object_name: public.table1
          value: 'id < 3'

  unavailable:
    id: 2
    type: my-platform
    params:
      db: db2
```

## Запуск тестов

Скопируйте пример конфигурации `test_config.example.yml` в `test_config.yml` и настройте
там значения параметров под тестируемый коннектор.

Запуск всех тестов:

```sh
$ ./run_tests.sh

test_health.py::test_health PASSED                                               [  3%]
test_object_data.py::test_object_data PASSED                                     [  7%]
test_object_data.py::test_missing_object_data PASSED                             [ 10%]
test_object_data.py::test_object_data_invalid_request PASSED                     [ 14%]
test_object_data.py::test_object_data_unavailable_data_source PASSED             [ 17%]
test_object_meta.py::test_object_meta PASSED                                     [ 21%]
test_object_meta.py::test_missing_object_meta PASSED                             [ 25%]
test_object_meta.py::test_object_meta_invalid_request PASSED                     [ 28%]
test_object_meta.py::test_object_meta_unavailable_data_source PASSED             [ 32%]
test_objects.py::test_objects_flat PASSED                                        [ 35%]
test_objects.py::test_objects_non_flat PASSED                                    [ 39%]
test_objects.py::test_objects_query_string_flat PASSED                           [ 42%]
test_objects.py::test_objects_query_string_flat_not_found PASSED                 [ 46%]
test_objects.py::test_objects_query_string_non_flat PASSED                       [ 50%]
test_objects.py::test_objects_query_string_non_flat_not_found PASSED             [ 53%]
test_objects.py::test_objects_unavailable PASSED                                 [ 57%]
test_parquet.py::test_parquet PASSED                                             [ 60%]
test_parquet.py::test_parquet_with_limit PASSED                                  [ 64%]
test_parquet.py::test_parquet_with_filters PASSED                                [ 67%]
test_parquet.py::test_parquet_sql PASSED                                         [ 71%]
test_parquet.py::test_parquet_sql_with_limit PASSED                              [ 75%]
test_parquet.py::test_missing_object_parquet PASSED                              [ 78%]
test_parquet.py::test_parquet_invalid_request PASSED                             [ 82%]
test_parquet.py::test_parquet_unavailable_data_source PASSED                     [ 85%]
test_ping.py::test_ping PASSED                                                   [ 89%]
test_ping.py::test_ping_unavailable PASSED                                       [ 92%]
test_sql_data.py::test_sql_data PASSED                                           [ 96%]
test_sql_meta.py::test_sql_meta PASSED                                           [100%]

================================== 28 passed in 4.96s ==================================
```

Запуск отдельных тестов:

```sh
$ ./run_tests.sh pytest -v test_object_data.py

test_object_data.py::test_object_data PASSED                                     [ 25%]
test_object_data.py::test_missing_object_data PASSED                             [ 50%]
test_object_data.py::test_object_data_invalid_request PASSED                     [ 75%]
test_object_data.py::test_object_data_unavailable_data_source PASSED             [100%]

================================== 4 passed in 0.15s ===================================


$ ./run_tests.sh pytest -v test_object_data.py::test_object_data

test_object_data.py::test_object_data PASSED                                     [100%]

================================== 1 passed in 0.08s ===================================
```

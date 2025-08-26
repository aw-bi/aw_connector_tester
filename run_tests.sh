#!/bin/bash

echo "Подготовка контейнера для тестирования..."
docker build -q -t aw-puller-tester .

docker run -it --rm --network host -v ./test_config.yml:/app/test_config.yml -w /app/src/aw_puller_tester aw-puller-tester "$@"

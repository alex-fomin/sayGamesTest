# Clickhouse

```
mkdir ~/some_clickhouse_database
docker run --name=clickhouse-test --volume=/Users/sane/some_clickhouse_database:/var/lib/clickhouse -p 0.0.0.0:8123:8123 -p 0.0.0.0:9000:9000 --expose=9009 -t yandex/clickhouse-server:latest
```

#TODO
1. Move connection string to config/env
1. Better error handling
1. Better logging
1. Monitoring
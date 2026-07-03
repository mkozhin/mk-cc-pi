# mk-cc-pi

Маркетплейс скиллов для [Claude Code](https://code.claude.com) и [Pi](https://pi.dev) — устанавливается одинаково в оба инструмента.

## Плагины и скиллы

### `mk-airflow`

Скиллы для работы с Apache Airflow.

| Скилл | Команда в Claude Code | Команда в Pi | Что делает |
|---|---|---|---|
| `dag-review` | `/mk-airflow:dag-review` | `/skill:mk-airflow-dag-review` | Статический анализ DAG-файлов Airflow (2.9+, ветка 2.x): корректность, best practices, отчёт с оценкой и рекомендациями |
| `debug` | `/mk-airflow:debug` | `/skill:mk-airflow-debug` | Диагностирует падения задач в Airflow: разбирает логи таски, доходит до Spark/YARN/Dataproc логов в S3, находит реальную причину ошибки |

## Установка

### Claude Code

Добавить маркетплейс (один раз):

```
/plugin marketplace add mkozhin/mk-cc-pi
```

Установить плагин:

```
/plugin install mk-airflow@mk-cc-pi
```

Обновление — вручную или автоматически при старте Claude Code:

```
/plugin marketplace update mk-cc-pi
```

### Pi

Установить пакет из git:

```
pi install git:mkozhin/mk-cc-pi
```

Обновление:

```
pi update --all
```

Версии в манифестах не проставляются намеренно — оба инструмента сами отслеживают изменения по коммитам в репозитории, так что `update`-команды выше всегда подтягивают актуальное состояние без ручного бампа версии.

---
name: mk-airflow-debug
description: >
  Диагностирует и находит причины падения задач в Apache Airflow.
  Используй этот скилл когда пользователь говорит что:
  - задача в Airflow упала / падает / завершилась с ошибкой
  - DAG run провалился
  - нужно разобраться почему Spark job не выполнился в Dataproc
  - нужно найти реальную причину ошибки "Unknown error" от Yandex Cloud
  - нужно найти логи Spark / YARN / Dataproc в S3
  - нужно отладить PySpark скрипт запущенный через Airflow
---

# Отладка падений задач в Airflow

## Необходимые инструменты

### 0. Сначала проверь — не подключён ли уже Airflow MCP

`astro-airflow-mcp` (тот же пакет, что и CLI `af` ниже) умеет работать и как MCP-сервер. Если пользователь уже подключил его в Claude Code, там уже прописаны `AIRFLOW_API_URL`/`AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` — не нужно ничего настраивать заново, и вызов инструмента будет быстрее, чем поднимать CLI. Поэтому прежде чем лезть в установку CLI, проверь MCP:

- Посмотри в списке доступных тебе инструментов (в т.ч. среди deferred/MCP-инструментов) имена вида `mcp__airflow__...`, `mcp__astro-airflow__...` или похожие — имя сервера пользователь мог задать произвольно.
- Либо спроси в терминале: `claude mcp list` — покажет настроенные MCP-серверы (без секретов в выводе).

Если такой MCP найден — используй его инструменты напрямую вместо `af` CLI из шага 1 (проверь их описания/схемы перед вызовом, конкретные имена и параметры зависят от версии сервера). К шагам 2+ (S3/Dataproc/YARN) это не относится — они не зависят от того, как получены Airflow-логи.

Если MCP не найден, переходи к CLI ниже. Заодно можно предложить пользователю подключить MCP на будущее — так следующий дебаг обойдётся вообще без ручной настройки:

```bash
claude mcp add airflow -s user \
  -e AIRFLOW_API_URL=https://<airflow-url> \
  -e AIRFLOW_USERNAME=<user> \
  -e AIRFLOW_PASSWORD='<password>' \
  -e AF_READ_ONLY=true \
  -- uvx astro-airflow-mcp --transport stdio
```

### 1. Airflow CLI (`astro-airflow-mcp`) — если MCP не подключён

Используется для чтения логов задач и информации о runs прямо из Airflow.

```bash
# Установка (не требует install, работает через uvx)
uvx --from astro-airflow-mcp af --help

# Настройка инстанса (один раз) — пароль через переменную окружения,
# а не CLI-флагом: аргументы командной строки попадают в историю шелла и в `ps`
export AIRFLOW_PASSWORD=<password>
uvx --from astro-airflow-mcp af instance add prod \
  --url https://<your-airflow-url> \
  --username <user>

# Проверить текущий инстанс
uvx --from astro-airflow-mcp af instance list
```

> Конфигурация сохраняется в `~/.af/config.yaml`.
> Альтернатива — переменные окружения целиком: `AIRFLOW_API_URL`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`.

### 2. MCP для S3 (Yandex S3 / AWS S3)

Нужен когда Airflow настроен писать логи в S3 (удалённое хранение логов).
В Claude Code подключается через MCP-сервер. Пример для Yandex S3:

```json
// ~/.claude.json или настройки проекта — секция mcpServers
{
  "mcpServers": {
    "yandex-s3": {
      "command": "...",
      "env": {
        "AWS_ACCESS_KEY_ID": "...",
        "AWS_SECRET_ACCESS_KEY": "...",
        "S3_ENDPOINT_URL": "https://storage.yandexcloud.net"
      }
    }
  }
}
```

После подключения MCP доступны инструменты: `s3_list_objects`, `s3_get_object`.

---

## Общий алгоритм диагностики

```
1. Получить логи задачи из Airflow
2. Найти реальную причину ошибки (не "Unknown error")
3. Если ошибка в Spark/Dataproc — найти логи в S3
4. Локализовать строку кода и причину
5. Предложить fix
```

---

## Шаг 1 — Получить логи задачи из Airflow

Если найден подключённый Airflow MCP (см. шаг 0) — вызови его инструмент получения логов задачи напрямую. Если нет — используй CLI:

```bash
uvx --from astro-airflow-mcp af tasks logs \
  <dag_id> \
  "<run_id>" \
  <task_id>
```

**Из URL Airflow** можно извлечь все параметры:
```
https://.../dags/<dag_id>/grid?dag_run_id=<run_id>&task_id=<task_id>
```
> `run_id` нужно URL-decode: `%3A` → `:`, `%2B` → `+`

**Что искать в логах:**
- Сообщение об ошибке верхнего уровня
- `cluster_id` и `job_id` (для Dataproc)
- Путь к логам в S3 (строка вида `*** Found logs in s3://...`)

---

## Шаг 2 — Spark / Dataproc ошибки

Когда Airflow показывает `Unknown error` от Yandex Cloud или `SparkException: Application finished with failed status` — это означает что сам Spark job упал. Настоящая причина — в логах Dataproc.

### Структура логов в S3

Dataproc пишет логи в бакет указанный в DAG (`YC_DP_LOGS_BUCKET` или `s3_bucket` в `DataprocCreateClusterOperator`).

```
<logs_bucket>/
  dataproc/
    clusters/
      <cluster_id>/
        jobs/
          <job_id>/
            driveroutput.000000000   ← stdout Spark driver (части по ~4KB)
            driveroutput.000000001
            ...
            driveroutput.XXXXXXXX    ← последний файл содержит финальный статус

  yarn-logs/
    dataproc-agent/
      bucket-logs-tfile/
        <node_id>/
          <yarn_app_id>/
            <hostname>_<port>        ← YARN container logs (бинарный TFile)
```

### Порядок поиска ошибки

**1. Найти `cluster_id` и `job_id`** из Airflow-лога:
```
INFO - Running Yandex.Cloud operation. ... cluster_id: "c9q..."  job_id: "c9q..."
```

**2. Прочитать последние файлы `driveroutput`:**

```python
# Через MCP s3_list_objects:
bucket = "<logs_bucket>"
prefix = f"dataproc/clusters/{cluster_id}/jobs/{job_id}/"

# Затем s3_get_object для последних 2-3 файлов
# Последний файл (меньший по размеру) содержит финальный статус
```

Если в driveroutput видно `User application exited with status 1` — Python-скрипт упал.
Реальный traceback будет в YARN logs.

**3. Найти Python traceback в YARN logs:**

Путь к финальному логу выстраивается по цепочке ID:

```
Airflow лог → cluster_id + job_id
    ↓
driveroutput файлы → yarn_app_id (строка вида "application_1776419699746_0001")
    ↓
YARN TFile → Python traceback
```

YARN TFile лежит по пути:
```
yarn-logs/dataproc-agent/bucket-logs-tfile/<node_id>/application_<yarn_app_id>/<hostname>_<port>
```

`node_id` — числовая папка (0001, 0002, …). Ищи сразу по app_id напрямую:

```python
# Берём yarn_app_id из driveroutput (строка "Submitted application application_XXXX_XXXX")
yarn_app_id = "application_1776419699746_0001"

# Пробуем узлы по порядку начиная с 0001
for node in ["0001", "0002", "0003", ...]:
    prefix = f"yarn-logs/dataproc-agent/bucket-logs-tfile/{node}/{yarn_app_id}/"
    result = s3_list_objects(bucket, prefix=prefix)
    if result['count'] > 0:
        key = result['objects'][0]['key']  # файл один на application_id
        break

# Читаем файл
data = s3_get_object(bucket, key)
content = data['content']
if data.get('is_base64'):
    import base64
    content = base64.b64decode(content).decode('utf-8', errors='replace')

# Ищем traceback
idx = content.find('Traceback (most recent call last)')
print(content[idx:idx+3000])
```

> Не листингуй весь `bucket-logs-tfile/` подряд — там 200+ файлов и пагинация.
> Ищи сразу по полному префиксу с `yarn_app_id`.

---

## Типовые ошибки и их причины

### FileNotFoundException: No such file or directory (S3)

```
java.io.FileNotFoundException:
No such file or directory: s3a://bucket/path/to/file.tsv
```

**Причина:** Race condition — файл был удалён или заменён другим процессом **во время** выполнения Spark job. Spark строит план чтения файлов в начале, а реально читает их позже (lazy evaluation).

**Диагностика:**
- Проверить через MCP `s3_list_objects` — файл существует сейчас?
- Если файл есть, но с другим именем (другая дата/timestamp в имени) — он был перезаписан
- Посмотреть `last_modified` текущего файла относительно времени запуска job

**Fix:**
1. Быстрый: перезапустить run (файл снова на месте)
2. Системный: разнести по времени расписание загрузки сырых данных и Spark job
3. Код: использовать `spark.read.option("ignoreCorruptFiles", "true")` если допустимо пропускать отдельные партиции

---

### SparkException: Job aborted / Stage failure

Общий признак: stage упал N раз на одной и той же таске.

**Шаги:**
1. Найти номер таски и stage в ошибке: `Task X in stage Y.Z failed 4 times`
2. Найти последнюю причину: `most recent failure: Lost task X.3 ... executor N`
3. Проверить YARN logs на OOM, network errors, Python exceptions

---

### OOM (Out of Memory) / Container killed

```
Container killed by YARN for exceeding memory limits
```

**Признаки:** `ExecutorLostFailure`, `Container killed`, `java.lang.OutOfMemoryError`

**Fix:** Увеличить `WORKERS` в DAG (ресурс кластера) или оптимизировать Spark job (repartition, persist, reduce shuffle).

---

### Python ImportError / ModuleNotFoundError

**Причина:** В Spark job используется библиотека, которой нет в образе Dataproc.

**Fix:** Передавать зависимости через `pyFiles` в `DataprocCreatePysparkJobOperator`.

---

### YARN app застрял в ACCEPTED (никогда не переходит в RUNNING)

```
INFO Client: Application report for application_XXX (state: ACCEPTED)
... (то же самое 10-50 минут подряд)
```

**Признаки:**
- Все driveroutput файлы содержат только `state: ACCEPTED`
- Airflow задача в итоге убита вручную или по timeout
- В Airflow логе: `State of this instance has been externally set to failed. Received SIGTERM`

**Причина:** YARN ResourceManager принял job, но не смог выделить ресурсы для запуска ApplicationMaster контейнера.

**Диагностика:**
1. Проверить сколько jobs запускалось на кластере одновременно: `s3_list_objects(bucket, prefix=f"dataproc/clusters/{cluster_id}/jobs/")`
2. Посмотреть `last_modified` предыдущего job — если предыдущий job завершился за секунды до старта нового, YARN мог не успеть освободить ресурсы
3. Сравнить `Will allocate AM container, with X MB memory` между текущим и предыдущим запуском — если резко выросло, значит данных стало больше и Spark запрашивает больше памяти

**Fix:**
1. **Быстрый:** перезапустить run
2. **Средний:** добавить паузу между тасками через `TimeDeltaSensor` или просто подождать перед ручным ретраем
3. **Системный:** проверить настройки Spark memory (`spark.driver.memory`, `spark.executor.memory`) — AM контейнер с 14+ GB это много для небольших кластеров

#### Частная причина: диск переполнен (YARN Disk Health)

YARN NodeManager проверяет заполненность диска. Если диск >90% — нода помечается `UNHEALTHY`, убирается из пула, и приложения зависают в ACCEPTED бессрочно.

**Когда подозревать:** предыдущий тяжёлый job только что завершился на том же кластере, диск небольшой (`DISK_SIZE` в DAG).

**Что происходит внутри (NodeManager logs):**
```
WARN NodeHealthCheckerService: Node is unhealthy: 1 of 1 local-dirs are bad
WARN NodeStatusUpdater: Disks are bad: /hadoop/yarn/nm-local-dir
```

**Как проверить:**
- Yandex Cloud Console → Dataproc → Clusters → Monitoring → Disk utilization
- Или посмотреть YARN RM logs в HDFS/Yandex Cloud Logging

**Fix:**
1. Увеличить `DISK_SIZE` в DAG (например с 40 до 100 GB)
2. Включить очистку shuffle/temp файлов между job-ами
3. Добавить паузу между тасками чтобы YARN успел почистить диск

---

## Быстрый чеклист при любом падении

```
[ ] 0. Есть ли уже подключённый Airflow MCP? Если да — использовать его вместо af CLI
[ ] 1. af tasks logs <dag> <run_id> <task_id> (или эквивалент через MCP)
[ ] 2. Найти cluster_id и job_id в логах (для Dataproc)
[ ] 3. s3_list_objects → dataproc/clusters/<cluster_id>/jobs/<job_id>/
[ ] 4. Прочитать последние 2-3 driveroutput файла
[ ] 5. Если "status 1" → взять yarn_app_id из driveroutput ("Submitted application application_XXXX")
[ ] 6. s3_list_objects → yarn-logs/dataproc-agent/bucket-logs-tfile/0001/application_{yarn_app_id}/ (пробовать 0001, 0002, … пока не найдёт файл)
[ ] 7. grep "Traceback" или "Exception" в YARN TFile (файл бинарный, is_base64=True — декодировать перед поиском)
[ ] 8. Определить тип ошибки → применить fix из раздела выше
```

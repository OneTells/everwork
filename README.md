# Everwork

Everwork — библиотека для асинхронной оркестрации воркеров, обрабатывающих события и сообщения. Она помогает управлять процессами,
отслеживать состояние воркеров и организовывать корректное завершение работы. В настоящее время поддерживает
работу с Redis Streams, но архитектура позволяет добавлять и другие брокеры сообщений.

## Возможности

- **Оркестрация воркеров** — управление жизненным циклом воркеров, их запуск, мониторинг и корректное завершение
- **Обработка событий** — поддержка триггеров (cron, interval) для периодического выполнения задач
- **Работа с сообщениями** — обработка сообщений из потоков данных (Redis Streams) с поддержкой consumer groups
- **Надёжность** — автоматическое обнаружение зависаний и перезапуск воркеров, настраиваемые стратегии повторных подключений
- **Мониторинг** — встроенная система отслеживания состояния воркеров и процессов
- **Расширяемость** — архитектура позволяет добавлять поддержку различных брокеров сообщений
- **Graceful shutdown** — многоэтапное корректное завершение работы с сохранением данных

## Требования окружения

- Python >= 3.12
- Linux с поддержкой multiprocessing start method `spawn` или `forkserver`
- Доступный экземпляр Redis (поддержка протокола RESP3)

## Быстрый старт

```python
import asyncio
from typing import Any

from everwork import AbstractWorker, Interval, Process, ProcessManager, RedisBackend, RedisBroker, Trigger, WorkerSettings


class ExampleWorker(AbstractWorker):

    @classmethod
    def _get_settings(cls) -> WorkerSettings:
        return WorkerSettings(
            title="example-worker",
            description="Пример воркера для обработки задач",
            execution_timeout=120,
            sources={"example-stream"},
            triggers=[
                Trigger(
                    title="periodic-task",
                    description="Периодическая задача каждые 30 секунд",
                    schedule=Interval(seconds=30),
                    kwargs={"message": "Hello from trigger!"}
                )
            ]
        )

    async def __call__(self, x: int = 0, **kwargs: Any) -> None:
        # Обработка входящих данных
        message = kwargs.get("message", "No message")
        print(f"Processing: {message}")

        # Здесь ваша бизнес-логика
        await asyncio.sleep(1)


async def main():
    manager = ProcessManager(
        uuid="unique-manager-uuid",
        processes=[
            Process(workers=[ExampleWorker])
        ],
        backend=RedisBackend("redis://localhost:6379/0"),
        broker=RedisBroker("redis://localhost:6379/0")
    )

    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())
```

## Архитектура и ключевые сущности

### Core компоненты

- **ProcessManager** — центральный менеджер, который регистрирует воркеров, создаёт consumer groups и поднимает отдельные
  процессы-наблюдатели для выполнения задач
- **AbstractWorker** — базовый класс для реализации воркеров. Каждый воркер описывает собственные настройки через метод `_get_settings()`
  и реализует асинхронный метод `__call__`. Также поддерживаются необязательные методы `startup()` и `shutdown()` для инициализации и очистки ресурсов
- **Process** — декларация процесса, объединяющая один или несколько воркеров. Управляет параметрами завершения (
  `shutdown_timeout`) и стратегией повторных подключений
- **ProcessGroup** — обёртка над `Process`, позволяющая запускать несколько реплик одного процесса

### WorkerSettings

Настройки воркера определяют его поведение и конфигурацию:

```python
@classmethod
def _get_settings(cls) -> WorkerSettings:
    return WorkerSettings(
        title="my-worker",                    # Уникальное название (обязательно)
        description="Описание воркера",       # Опциональное описание (по умолчанию: "")
        sources={"stream1", "stream2"},       # Источники сообщений (set, по умолчанию: auto)
        execution_timeout=180,                # Таймаут выполнения задачи в секундах (0.1-86400, по умолчанию: 180)
        status_check_interval=60,             # Интервал проверки статуса в секундах (0.1-3600, по умолчанию: 60)
        status_cache_ttl=5,                   # TTL кеша статуса в секундах (>=0, None = без кеша, по умолчанию: 5)
        event_settings=EventSettings(        # Настройки обработки событий
            max_events_in_memory=5000,        # Максимум событий в памяти (1-10000, по умолчанию: 5000)
            max_batch_size=500               # Максимальный размер пакета (1-10000, по умолчанию: 500)
        ),
        triggers=[...]                         # Список триггеров (по умолчанию: [])
    )
```

#### Параметры WorkerSettings:

- **title** (обязательный) — уникальное название воркера, используется для генерации ID
- **description** — текстовое описание воркера (максимум 1000 символов)
- **sources** — множество источников сообщений для чтения. Автоматически добавляется `worker:{id}:source`
- **execution_timeout** — максимальное время выполнения одной задачи в секундах
- **status_check_interval** — как часто проверять статус воркера
- **status_cache_ttl** — время жизни кеша статуса. `None` отключает кеширование
- **event_settings** — настройки для обработки событий (внутреннее использование)
- **triggers** — список триггеров для периодического запуска воркера

#### Автоматически генерируемые поля:

- **id** — уникальный идентификатор воркера (первые 16 символов SHA256 от title)
- **default_source** — источник по умолчанию в формате `worker:{id}:source`

#### Валидация:

- Все триггеры должны иметь уникальные названия
- Источники должны соответствовать паттерну `^[a-zA-Z0-9_\-:]+$`
- Максимальное количество источников — 100

### Триггеры

Триггеры позволяют запускать воркеры периодически по расписанию:

```python
from everwork import Trigger, Interval, Cron

# Interval trigger - каждые N времени
Trigger(
    title="daily-cleanup",
    description="Ежедневная очистка данных",
    schedule=Interval(hours=24),
    kwargs={"cleanup_type": "full"},
    is_catchup=True,
    lifetime=86400  # 24 часа
)

# Cron trigger - по cron-выражению
Trigger(
    title="weekly-report", 
    description="Еженедельный отчёт",
    schedule=Cron(expression="0 9 * * 1"),  # Каждый понедельник в 9:00
    kwargs={"report_type": "weekly"},
    is_catchup=False,
    status_check_interval=30
)
```

#### Типы расписаний:

**Interval** — запуск через промежутки времени:
```python
Interval(
    weeks=1,        # недели (>=0)
    days=2,         # дни (>=0) 
    hours=3,        # часы (>=0)
    minutes=15,     # минуты (>=0)
    seconds=30      # секунды (>=0)
)
```

**Cron** — запуск по cron-выражению:
```python
Cron(expression="0 9 * * 1")  # Стандартный формат cron
```

#### Параметры Trigger:

- **title** (обязательный) — уникальное название триггера (1-300 символов)
- **description** — описание триггера (максимум 1000 символов, по умолчанию: "")
- **schedule** — расписание (Interval или Cron)
- **kwargs** — параметры, передаваемые в воркер при запуске (по умолчанию: {})
- **is_catchup** — выполнять ли пропущенные запуски (по умолчанию: True)
- **lifetime** — время жизни триггера в секундах (None = бессрочно, по умолчанию: None)
- **status_check_interval** — интервал проверки статуса в секундах (0.1-3600, по умолчанию: 60)
- **status_cache_ttl** — TTL кеша статуса в секундах (>=0, None = без кеша, по умолчанию: 5)

#### Автоматически генерируемые поля:

- **id** — уникальный идентификатор триггера (первые 16 символов SHA256 от title)

#### Важные особенности:

- Все триггеры в рамках одного воркера должны иметь уникальные названия
- При `is_catchup=True` система выполнит пропущенные запуски, если воркер был недоступен
- `lifetime` ограничивает время работы триггера — после истечения он перестаёт срабатывать
- Параметры из `kwargs` передаются в метод `__call__` воркера при каждом запуске

### Конфигурация ProcessManager

```python
manager = ProcessManager(
    uuid="unique-manager-uuid",              # Уникальный идентификатор менеджера
    processes=[Process(...)],                # Список процессов для запуска
    backend=RedisBackend("redis://..."),    # Бэкенд для управления состоянием
    broker=RedisBroker("redis://...")        # Брокер для работы с сообщениями
)
```

## Репликация через ProcessGroup

```python
from everwork import Process, ProcessGroup

process = Process(workers=[ExampleWorker])
group = ProcessGroup(process=process, replicas=3)
```

ProcessGroup позволяет запускать несколько реплик одного процесса для повышения производительности и отказоустойчивости.

## Жизненный цикл воркера

### Методы жизненного цикла

```python
class MyWorker(AbstractWorker):

    async def startup(self) -> None:
        # Вызывается при старте воркера
        # Инициализация ресурсов, подключений и т.д.
        pass
    
    async def __call__(self, **kwargs: Any) -> None:
        # Основная логика обработки
        pass
    
    async def shutdown(self) -> None:
        # Вызывается при завершении воркера
        # Очистка ресурсов, закрытие подключений
        pass
```

## Завершение воркеров

Процесс завершения проходит поэтапно и зависит от настроек `WorkerSettings.execution_timeout` и `Process.shutdown_timeout`:

1. **Мягкое завершение.** Менеджер отправляет воркер-процессу сигнал завершения и ждёт окончания текущей задачи. Таймаут ожидания
   равен `execution_timeout` текущего воркера. Если задача завершилась — shutting down продолжается без принуждения
2. **Принудительная отмена задачи.** Если воркер не уложился в `execution_timeout`, отправляется второй сигнал. Он вызывает
   `asyncio.CancelledError` или `KeyboardInterrupt` внутри выполняемого `__call__`. Исключение нужно корректно обрабатывать 
   (например, очищать ресурсы), но его нельзя подавлять
3. **Ждём завершение процесса.** После отмены даётся ещё `shutdown_timeout` на работу `shutdown()` и финализацию ресурсов
4. **SIGKILL как крайняя мера.** Если процесс всё ещё не остановился, отправляется `SIGKILL`. Это может оставить ресурсы в
   неконсистентном состоянии, поэтому после получения такого сигнала рекомендуется перезапустить весь контейнер/оркестрацию

Такой алгоритм обеспечивает graceful shutdown при штатной работе и даёт возможность безопасно восстановиться после зависаний.

## Обработка событий

### Работа с потоками и `run_in_executor`

При использовании внутри воркера `asyncio.run_in_executor`, `asyncio.to_thread` или явного запуска потоков через модуль
`threading` стоит учитывать, что эти потоки не получают сигналы `SIGUSR1`/`SIGTERM`. В результате они не смогут корректно
завершиться вместе с основным воркером. Если вам необходимо выполнять блокирующие операции, предпочтительнее:

- ограничивать их временем выполнения и прерывать вручную
- передавать сигналы завершения через собственные флаги/события
- обеспечивать корректную очистку ресурсов при обработке `asyncio.CancelledError` в основном потоке выполнения воркера

## Обработка ошибок

Обработка события происходит в несколько этапов:

1. **Получение события.** При чтении из потока данных формируется событие воркера. Оно содержит сообщение и его аргументы. Если
   воркер работает по триггеру, создаётся событие без сообщения и аргументов
2. **Ошибки при обработке.** Если воркер выбросил исключение или задача не завершилась за время `execution_timeout`, это
   фиксируется в логах (`logger.exception`). В таком случае система сообщает об ошибке и, если у события было сообщение,
   подтверждает его через механизм подтверждения брокера, чтобы оно не зависло в потоке
3. **Успешная обработка.** Если событие содержало сообщение из потока, по результатам обработки оно подтверждается через брокер
4. **Отмена задачи.** Если событие содержало сообщение из потока, при отмене выполнения оно возвращается в исходный поток
   через специальный скрипт: запись помечается как обработанная и повторно помещается в персональный поток воркера (
   `<worker-id>:stream`). Это позволяет восстановить обработку без потери данных

#### Управление выполнением задачи

Воркер может управлять дальнейшей судьбой события через специальные исключения:

```python
from everwork.exceptions import Fail, Reject, Retry

async def __call__(self, **kwargs):
    try:
        # Логика обработки
        await process_data()
        
        # Успешное выполнение - автоматический ACK
        return
        
    except ValueError as e:
        # Критическая ошибка - не повторять попытки
        raise Fail("Некорректные данные: невозможно обработать")
        
    except TemporaryUnavailableError:
        # Временная проблема - повторить позже
        raise Retry
        
    except BusinessLogicError:
        # Некорректное сообщение - отклонить без повтора
        raise Reject
```

#### Типы ответов воркера:

- **ACK (по умолчанию)** — успешное выполнение, сообщение подтверждается и удаляется из потока
- **Fail** — критическая ошибка обработки. Сообщение подтверждается (не будет повторяться), но ошибка логируется. 
  Используется для невосстановимых ошибок (невалидные данные, ошибки конфигурации)
- **Reject** — отклонение сообщения без повтора. Сообщение подтверждается и удаляется. 
  Используется для некорректных сообщений, которые не имеют смысла повторять
- **Retry** — запрос на повторное выполнение. Сообщение возвращается в поток для последующей обработки.
  Используется для временных проблем (сетевые ошибки, недоступность внешних сервисов)

#### Механизмы подтверждения:

Брокер обеспечивает надёжную доставку сообщений через:
- **Подтверждение обработки** — успешная обработка сообщения подтверждается через механизм брокера
- **Возврат для повтора** — при отмене или ошибке сообщение возвращается для повторной обработки
- **Персональные потоки** — каждый воркер имеет свой поток для восстановления обработки

#### Логирование ошибок:

Все ошибки обработки логируются с контекстом:
- UUID процесса
- ID воркера  
- Детали исключения
- Время возникновения ошибки

## Продвинутые возможности

### Кастомные планировщики cron

Вы можете использовать собственную реализацию cron-планировщика:

```python
from everwork import AbstractCronSchedule

class CustomCronSchedule(AbstractCronSchedule):
    def next_execution(self, cron_expression: str) -> datetime:
        # Ваша логика расчёта следующего времени выполнения
        pass

manager = ProcessManager(
    # ... другие параметры
    cron_schedule=CustomCronSchedule
)
```

### Мониторинг и метрики

Библиотека предоставляет встроенные возможности для мониторинга:

- Статус воркеров доступен через Redis
- Автоматическая проверка зависших процессов
- Логирование всех критических событий

### Конфигурация Redis

Для оптимальной производительности рекомендуется настроить Redis:

```redis
# Настройки для работы с streams
databases 16
maxmemory 512mb
maxmemory-policy allkeys-lru
appendonly yes
appendfsync everysec
save 900 1
save 300 10
save 60 10000
tcp-keepalive 300
loglevel notice
protected-mode yes
```

## Рекомендуемый Dockerfile

Для упрощения билда и повторяемости окружения рекомендуется использовать следующий Dockerfile:

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y git gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-editable --no-install-project --no-dev

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --no-dev

FROM python:3.14-slim-bookworm AS production

RUN groupadd --system --gid 999 nonroot  \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

ENV PYTHONUNBUFFERED=1 PYTHONOPTIMIZE=2
ENV TZ=Europe/Moscow

COPY --from=builder --chown=nonroot:nonroot /app/.venv /app/.venv
COPY --from=builder --chown=nonroot:nonroot /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH"

USER nonroot
WORKDIR /app/src
```

## Docker Compose

```yaml
services:
  redis:
    image: redis:8.0-alpine
    command: >
      sh -c "
        echo 'requirepass '$$REDIS_PASSWORD > /tmp/redis.conf &&
        cat /usr/local/etc/redis/redis.conf >> /tmp/redis.conf &&
        redis-server /tmp/redis.conf
      "
    ports:
      - "6379:6379"
    environment:
      REDIS_PASSWORD: ${REDIS_PASSWORD}
    volumes:
      - redis_data:/data
      - ./redis.conf:/usr/local/etc/redis/redis.conf
    healthcheck:
      test: [ "CMD", "redis-cli", "--no-auth-warning", "-a", "${REDIS_PASSWORD}", "ping" ]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 10s
    restart: unless-stopped
    stop_grace_period: 10s

  workers:
    build: .
    entrypoint: python workers.py
    depends_on:
      redis:
        condition: service_healthy
    stop_grace_period: 60s
    stop_signal: SIGINT
    restart: unless-stopped

volumes:
  redis_data:

```

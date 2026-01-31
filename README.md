# Everwork

Everwork — библиотека для оркестрации воркеров, работающих с очередями Redis Streams. Она помогает управлять процессами,
отслеживать состояние воркеров и организовывать корректное завершение работы.

## Возможности

- Регистрация и управление группами процессов и воркеров.
- Автоматическое создание consumer group в Redis Streams.
- Управление жизненным циклом воркеров (инициализация, graceful shutdown).
- Обнаружение зависаний и автоматический перезапуск процессов с воркерами.
- Настраиваемые стратегии повторных подключений к Redis.

## Требования окружения

- Python \>= 3.12.
- Linux с поддержкой multiprocessing start method `spawn` или `forkserver`.
- Доступный экземпляр Redis (поддержка протокола RESP3).

## Установка

```bash
pip install everwork
```

### Установка через uv

```bash
uv add everwork
uv sync
```

После добавления зависимости выполните `uv run` или `uvx` для запуска утилит внутри управляемого окружения.

## Быстрый старт

```python


from everwork import AbstractBackend, AbstractBroker, AbstractWorker, Process, ProcessManager, WorkerSettings


class ExampleWorker(AbstractWorker):

    @classmethod
    def _get_settings(cls) -> WorkerSettings:
        return WorkerSettings(
            slug="example",
            execution_timeout=120,
        )

    async def __call__(self, **kwargs):
        ...


manager = ProcessManager(
    uuid="<uuid>",
    processes=[Process(workers=[ExampleWorker])],
    backend_factory=lambda: AbstractBackend("redis://localhost:6379/0"),
    broker_factory=lambda: AbstractBroker("redis://localhost:6379/0"),
)
```

## Архитектура и ключевые сущности

- **ProcessManager** — центральный менеджер, который регистрирует воркеров в Redis, создаёт consumer groups и поднимает отдельные
  процессы-наблюдатели для выполнения задач.
- **AbstractWorker** — базовый класс для реализации воркеров. Каждый воркер описывает собственные настройки через `WorkerSettings`
  и реализует асинхронный метод `__call__`.
- **Process** — декларация процесса, объединяющая один или несколько воркеров. Управляет параметрами завершения (
  `shutdown_timeout`) и стратегией повторных подключений к Redis.
- **ProcessGroup** — обёртка над `Process`, позволяющая запускать несколько реплик одного процесса. Репликация работает только
  если процесс содержит одного воркера и он не использует `TriggerMode`.

### Репликация через ProcessGroup

```python
from everwork import Process, ProcessGroup

process = Process(workers=[])
group = ProcessGroup(process=process, replicas=3)
```

- На каждый экземпляр создаётся отдельный процесс с полным копированием настроек воркера.
- Благодаря ограничению в один воркер на процесс и запрету `TriggerMode`, реплики безопасно читают одну и ту же очередь без
  конфликтов.
- В сочетании с `Process.shutdown_timeout` и `WorkerSettings.execution_timeout` репликация остаётся предсказуемой: при зависаниях
  менеджер перезапускает конкретный процесс, не задевая остальные.

## Завершение воркеров

Процесс завершения проходит поэтапно и зависит от настроек `WorkerSettings.execution_timeout` и `Process.shutdown_timeout`:

1. **Мягкое завершение.** Менеджер отправляет воркер-процессу сигнал завершения и ждёт окончания текущей задачи. Таймаут ожидания
   равен `execution_timeout` текущего воркера. Если задача завершилась — shutting down продолжается без принуждения.
2. **Принудительная отмена задачи.** Если воркер не уложился в `execution_timeout`, отправляется второй сигнал. Он вызывает
   `asyncio.CancelledError` или `KeyboardInterrupt` внутри выполняемого `__call__`. Исключение нужно корректно обрабатывать 
   (например, очищать ресурсы), но его нельзя подавлять.
3. **Ждём завершение процесса.** После отмены даётся ещё `shutdown_timeout` на работу `shutdown()` и финализацию ресурсов.
4. **SIGKILL как крайняя мера.** Если процесс всё ещё не остановился, отправляется `SIGKILL`. Это может оставить ресурсы в
   неконсистентном состоянии, поэтому после получения такого сигнала рекомендуется перезапустить весь контейнер/оркестрацию.

Такой алгоритм обеспечивает graceful shutdown при штатной работе и даёт возможность безопасно восстановиться после зависаний.

### Работа с потоками и `run_in_executor`

При использовании внутри воркера `asyncio.run_in_executor`, `asyncio.to_thread` или явного запуска потоков через модуль
`threading` стоит учитывать, что эти потоки не получают сигналы `SIGUSR1`/`SIGTERM`. В результате они не смогут корректно
завершиться вместе с основным воркером. Если вам необходимо выполнять блокирующие операции, предпочтительнее:

- ограничивать их временем выполнения и прерывать вручную;
- передавать сигналы завершения через собственные флаги/события;
- обеспечивать корректную очистку ресурсов при обработке `asyncio.CancelledError` в основном потоке выполнения воркера.

## Обработка ивента воркером

Обработка ивента происходит в несколько этапов:

1. **Получение события.** При чтении из Redis Streams формируется событие воркера. Оно содержит сообщение и его аргументы. Если
   воркер работает по триггеру, создаётся событие без сообщения и аргументов.
2. **Ошибки при обработке.** Если воркер выбросил исключение или задача не завершилась за время `execution_timeout`, это
   фиксируется в логах (`logger.exception`). В таком случае система сообщает об ошибке и, если у события было сообщение,
   подтверждает его через `XACK`, чтобы оно не зависло в очереди.
3. **Успешная обработка.** Если событие содержало сообщение из стрима, по результатам обработки оно подтверждается (`XACK`).
4. **Отмена задачи.** Если событие содержало сообщение из стрима, при отмене выполнения оно возвращается в исходный поток
   через Lua-скрипт: запись помечается как обработанная и повторно помещается в персональную очередь воркера (
   `<slug>:stream`). Это позволяет восстановить обработку без потери данных.

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

## docker-compose

Для корректного завершения процессов при использовании `docker compose` рекомендуется настраивать `stop_grace_period` не менее
`60s`. Оптимальное значение — сумма максимального `execution_timeout` среди воркеров и `shutdown_timeout` процесса плюс
дополнительный запас в 2 секунды:

```
stop_grace_period = max(worker.settings.execution_timeout) + process.shutdown_timeout + 2s
```

Это гарантирует корректное завершение фоновых задач без принудительного обрыва.

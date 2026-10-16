# adaptive-microservice

`adaptive-microservice` — высокопроизводительный микросервис на Rust, реализующий Hexagonal (Clean) Architecture. Предоставляет REST API для управления динамическими сущностями с поддержкой версионирования схем.

## Возможности

- **REST API**: Создание, получение, обновление и удаление сущностей через HTTP (Axum).
- **Адаптивное хранилище**: PostgreSQL (основное хранение) + Redis (кэширование или основное хранилище).
- **Событийная модель**: Публикация доменных событий в Kafka при изменении сущностей.
- **Версионирование схем**: Поддержка эволюции схемы данных и автоматическая миграция при чтении.
- **Наблюдаемость (Observability)**: Структурированное JSON-логирование, сбор логов через Promtail → Loki и визуализация в Grafana. Метрики через Prometheus.

## Требования

- [Docker Engine](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

## Конфигурация

Приложение читает настройки из файла `config.yaml` (или `config/config.yaml`). Пример актуальной конфигурации:

```yaml
api:
  host: "0.0.0.0"
  port: 3000

postgres:
  dsn: "postgres://dev:dev@postgres:5432/app_dev"
  table: "entities"

redis:
  url: "redis://redis:6379"
  mode: "cache" # или "storage"
  ttl: 3600

kafka:
  brokers: "kafka:19092"
  group: "service-group"

schemas:
  - name: "user"
    current_version: 3
    versions:
      - version: 1
        fields:
          name: { type: "string", required: true }
      - version: 2
        fields:
          name: { type: "string", required: true }
          email: { type: "string", required: false }
        migration_from: 1
      - version: 3
        fields:
          name: { type: "string", required: true }
          email: { type: "string", required: true }
          phone: { type: "string", required: false }
        migration_from: 2
```

---

## Запуск

### Способ 1: Полное окружение разработки (Рекомендуется)
Запускает приложение вместе со всеми зависимостями (Postgres, Redis, Kafka) и стеком мониторинга (Grafana, Loki, Prometheus, Kafka UI).

```bash
# Сборка и запуск
docker compose -f docker-compose.dev.yaml up -d --build

# Проверка статуса
docker compose -f docker-compose.dev.yaml ps
```

### Способ 2: Запуск только образа приложения
Если базы данных и Kafka уже развернуты в вашей инфраструктуре, можно запустить только контейнер приложения:

```bash
docker build -t adaptive-microservice:latest .

docker run -d \
  --name adaptive-microservice \
  --restart unless-stopped \
  -p 3000:3000 \
  -v "$(pwd)/config.yaml:/app/config.yaml:ro" \
  adaptive-microservice:latest
```

---

## HTTP API

Базовый адрес: `http://localhost:3000`

**1. Проверка состояния (Health Check)**
```bash
curl http://localhost:3000/health
# Ответ: OK
```

**2. Создание сущности**
```bash
curl -X POST http://localhost:3000/api/v1/entities \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'
```

**3. Получение списка сущностей**
```bash
curl "http://localhost:3000/api/v1/entities?offset=0&limit=10"
```

**4. Получение сущности по ID**
```bash
curl http://localhost:3000/api/v1/entities/<ENTITY_ID>
```

**5. Обновление сущности**
```bash
curl -X PUT http://localhost:3000/api/v1/entities/<ENTITY_ID> \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Updated", "email": "alice@example.com", "phone": "+123456789"}'
```

**6. Удаление сущности**
```bash
curl -X DELETE http://localhost:3000/api/v1/entities/<ENTITY_ID>
```

---

## События (Kafka)

При создании, обновлении или удалении сущности сервис публикует событие в топик Kafka.
- **Топик**: `{schema_name}.events` (например, `user.events`).
- **Формат сообщения**:
  ```json
  {
    "event_type": "EntityCreated",
    "entity_id": "550e8400-e29b-41d4-a716-446655440000",
    "schema_version": 3,
    "payload": {"name": "Alice", "email": "alice@example.com"},
    "timestamp": "2026-10-15T12:00:00Z"
  }
  ```

**Просмотр событий**: Откройте [Kafka UI](http://localhost:8080), перейдите в раздел **Topics** → `user.events` → **Messages**.

---

## Логирование и Наблюдаемость (Observability)

Приложение генерирует структурированные логи в формате JSON, которые автоматически собираются стеком **Promtail → Loki → Grafana**.

### Доступ к Grafana
- **URL**: [http://localhost:3001](http://localhost:3001)
- **Логин**: `admin`
- **Пароль**: `admin`

### Полезные запросы LogQL (в разделе Explore)

**Все логи приложения:**
```logql
{container=~".*app.*"} | json
```

**Только ошибки (ERROR):**
```logql
{container=~".*app.*"} | json | level="ERROR"
```

**Поиск по тексту (например, успешное создание):**
```logql
{container=~".*app.*"} |= "EntityCreated"
```

**Частота ошибок в минуту:**
```logql
rate({container=~".*app.*"} | json | level="ERROR"[1m])
```

### Метрики (Prometheus)
- **URL**: [http://localhost:9090](http://localhost:9090)
- Приложение экспортирует метрики по пути `/metrics` (порт 3000).

---

## Диагностика и устранение неполадок

Если приложение не отвечает или контейнер падает (`Exited`):

1. **Проверьте логи приложения**:
   ```bash
   docker compose -f docker-compose.dev.yaml logs -f app
   ```
2. **Убедитесь, что зависимости здоровы**:
   ```bash
   docker compose -f docker-compose.dev.yaml ps
   # Все сервисы (postgres, redis, kafka) должны иметь статус (healthy)
   ```
3. **Проверьте файл конфигурации**: Убедитесь, что `config.yaml` существует, валиден (YAML) и пути в нем соответствуют именам сервисов в Docker-сети (например, `postgres:5432`, а не `localhost:5432`).


## Лицензия

MIT License. См. файл `LICENSE` для подробностей.
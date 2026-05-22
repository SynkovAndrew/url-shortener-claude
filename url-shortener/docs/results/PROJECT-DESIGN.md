# URL Shortener — Project Design

Дизайн-документ учебного сервиса сокращения ссылок. Стек: **Java 21, Spring Boot, Apache Kafka, PostgreSQL**, spec-first OpenAPI.

## Применяемые принципы

**Архитектурные:** REST API, layered architecture (Controller → Service → Repository), SOLID, 12-Factor App, DDD-lite (выделение домена без тяжёлой машинерии агрегатов).

**Производительность и масштабируемость:**
- Идемпотентное shortening (один и тот же long URL → один и тот же short code).
- Async-аналитика через Kafka (клики не блокируют редирект).
- Пагинация и лимиты на list-эндпоинтах.

**Операбельность и качество:** DB-миграции (Liquibase), observability (структурированные логи + Micrometer + OpenTelemetry), testing pyramid (unit + Testcontainers), graceful shutdown, containerization (docker-compose).

**Дополнительно:** **spec-first OpenAPI** — REST контроллеры и DTO генерируются из YAML-спеки.

> Caching и все security-вопросы вынесены в раздел **Future features** и в MVP не реализуются.

---

## 1. Архитектура и компоненты

### Высокоуровневая диаграмма

```
                ┌────────────────────────────────────────────────┐
                │                Client (browser / curl)         │
                └───────────────┬────────────────┬───────────────┘
                                │                │
                  POST /api/v1/urls       GET /{shortCode}
                                │                │
                                ▼                ▼
                ┌────────────────────────────────────────────────┐
                │   url-shortener (Spring Boot, Java 21)         │
                │                                                │
                │  ┌──────────────┐   ┌────────────────────────┐ │
                │  │ ShortenerCtl │   │ RedirectController     │ │
                │  └──────┬───────┘   └────────┬───────────────┘ │
                │         │                    │                 │
                │         ▼                    ▼                 │
                │  ┌──────────────┐   ┌────────────────────────┐ │
                │  │ UrlService   │   │ RedirectService        │ │
                │  └──────┬───────┘   └────────┬───────────────┘ │
                │         │                    │                 │
                │         ▼                    ▼                 │
                │  ┌──────────────┐   ┌────────────────────────┐ │
                │  │ UrlRepository│   │ ClickEventPublisher    │ │
                │  └──────┬───────┘   └────────┬───────────────┘ │
                └─────────┼─────────────────────┼────────────────┘
                          │                     │
                          ▼                     ▼
                  ┌──────────────┐      ┌──────────────────────┐
                  │  PostgreSQL  │      │        Kafka         │
                  │   (urls,     │      │  url.created.v1      │
                  │   click_stats)│     │  url.clicked.v1      │
                  └──────▲───────┘      └──────────┬───────────┘
                         │                         │
                         │                         ▼
                         │                ┌─────────────────────┐
                         └────────────────┤ ClickStatsConsumer  │
                              UPSERT      │ (same Spring app,   │
                            click_stats   │  separate component)│
                                          └─────────────────────┘
```

### Компоненты

| Компонент | Назначение |
|---|---|
| **REST API (Spring MVC)** | Два контроллера: shortening и redirect. Контракты генерируются из OpenAPI спеки. |
| **Domain/Service layer** | `UrlService` (идемпотентное создание), `RedirectService` (резолв + publish клика). |
| **PostgreSQL** | Источник правды для маппингов и агрегированной статистики. Миграции — Liquibase. |
| **Kafka** | Транспорт для доменных событий. **Один топик на тип события** (`url.created.v1`, `url.clicked.v1`, …). |
| **ClickStatsConsumer** | Kafka listener в том же Spring Boot процессе, аггрегирует клики в `click_stats`. |

### Технологический стек

- **Java 21** (records, pattern matching, virtual threads для Kafka listener).
- **Spring Boot 3.4+** (`spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-kafka`, `spring-boot-starter-actuator`).
- **PostgreSQL 16** + **Liquibase**.
- **Apache Kafka** (KRaft mode, без Zookeeper).
- **Gradle (Kotlin DSL)**.
- **openapi-generator-gradle-plugin** — кодоген серверных интерфейсов и DTO.
- **Testcontainers** — Postgres + Kafka для интеграционных тестов.
- **Micrometer + OpenTelemetry** — метрики и трейсы.
- **docker-compose** для локального запуска (app + postgres + kafka).

### Подход к OpenAPI (spec-first)

- Спека хранится в `src/main/resources/openapi/url-shortener.yaml`.
- `openapi-generator-gradle-plugin` на этапе сборки генерирует:
  - **Server stubs** — Java интерфейсы контроллеров (`generatorName: spring`, `interfaceOnly: true`, `useSpringBoot3: true`). Их реализуют наши `@RestController`-классы.
  - **DTO** — request/response модели.
- Сгенерированные классы лежат в `build/generated/...`, добавляются в `sourceSets`, не коммитятся.

### Зафиксированные решения

1. **Единый Spring Boot процесс** для API и Kafka-consumer. При росте легко вынести consumer в отдельный модуль/сервис.
2. **Топик на тип события**, имя по схеме `<domain>.<event>.v<version>`. Список топиков детализируется в Секции 5.
3. **Liquibase** для миграций (XML/YAML changelog'и, выразительные rollback'и).
4. **Kafka в KRaft-режиме** (без Zookeeper) в docker-compose.

---

## 2. REST API и OpenAPI спека

### Эндпоинты MVP

| Метод | Путь | Назначение | Ответ |
|---|---|---|---|
| `POST` | `/api/v1/urls` | Создать короткую ссылку (идемпотентно) | `201 Created` (новая) \| `200 OK` (уже существует) + `ShortenedUrl` |
| `GET`  | `/api/v1/urls/{shortCode}` | Получить детали маппинга | `200 OK` + `ShortenedUrl` \| `404` |
| `GET`  | `/api/v1/urls/{shortCode}/stats` | Агрегированная статистика кликов | `200 OK` + `UrlStats` \| `404` |
| `GET`  | `/{shortCode}` | **Редирект** на оригинальный URL | `302 Found` + `Location`, асинхронная публикация click-события |

> Редирект живёт в корне (`/{shortCode}`), а не под `/api/v1/...` — это user-facing короткая ссылка.

### Модели данных (DTO)

**`CreateShortUrlRequest`:**
```json
{ "longUrl": "https://example.com/some/very/long/path?with=query" }
```

**`ShortenedUrl`:**
```json
{
  "shortCode": "aB3xZ9",
  "shortUrl": "https://sho.rt/aB3xZ9",
  "longUrl": "https://example.com/...",
  "createdAt": "2026-05-22T10:15:30Z"
}
```

**`UrlStats`:**
```json
{
  "shortCode": "aB3xZ9",
  "totalClicks": 1423,
  "lastClickAt": "2026-05-22T11:42:01Z"
}
```

**`ErrorResponse`** — формат **RFC 7807 Problem Details**:
```json
{
  "type": "https://urlsho.rt/errors/not-found",
  "title": "Short code not found",
  "status": 404,
  "detail": "No mapping for shortCode=aB3xZ9",
  "instance": "/api/v1/urls/aB3xZ9"
}
```

### Layout OpenAPI спеки

`src/main/resources/openapi/url-shortener.yaml`:

```yaml
openapi: 3.0.3
info:
  title: URL Shortener API
  version: 1.0.0
servers:
  - url: http://localhost:8080
tags:
  - name: urls
  - name: redirect
paths:
  /api/v1/urls:
    post: { tags: [urls], operationId: createShortUrl, ... }
  /api/v1/urls/{shortCode}:
    get:  { tags: [urls], operationId: getShortUrl, ... }
  /api/v1/urls/{shortCode}/stats:
    get:  { tags: [urls], operationId: getUrlStats, ... }
  /{shortCode}:
    get:  { tags: [redirect], operationId: redirectToLongUrl, responses: { '302': ... } }
components:
  schemas:
    CreateShortUrlRequest: ...
    ShortenedUrl: ...
    UrlStats: ...
    ErrorResponse: ...
```

### Конфиг openapi-generator-gradle-plugin

В `build.gradle.kts`:

```kotlin
plugins {
  id("org.openapi.generator") version "7.7.0"
}

openApiGenerate {
  generatorName.set("spring")
  inputSpec.set("$rootDir/src/main/resources/openapi/url-shortener.yaml")
  outputDir.set("$buildDir/generated/openapi")
  apiPackage.set("com.example.urlshortener.api")
  modelPackage.set("com.example.urlshortener.api.model")
  configOptions.set(mapOf(
    "interfaceOnly" to "true",
    "useSpringBoot3" to "true",
    "useJakartaEe" to "true",
    "useTags" to "true",
    "dateLibrary" to "java8"
  ))
}
sourceSets["main"].java.srcDir("$buildDir/generated/openapi/src/main/java")
tasks.named("compileJava") { dependsOn("openApiGenerate") }
```

Каждый `@RestController` реализует сгенерированный интерфейс:
```java
@RestController
class ShortenerController implements UrlsApi { /* ... */ }
```

### Поведенческие решения

- **Идемпотентность `POST /api/v1/urls`** — если `longUrl` уже зашорчен, возвращаем существующий маппинг с `200 OK`; иначе `201 Created` + `Location: /{shortCode}`.
- **`302 Found` для редиректа** (не `301`), чтобы каждый клик долетал до сервиса и попадал в статистику.
- **Версионирование через URI** (`/api/v1/...`), а не через заголовок — проще для обучения и для curl/браузера.
- **operationId**-ы: `createShortUrl`, `getShortUrl`, `getUrlStats`, `redirectToLongUrl`.

---

## 3. Data model & DB schema

### Таблица `urls` — источник правды для маппингов

| Колонка | Тип | Описание |
|---|---|---|
| `id` | `UUID PRIMARY KEY DEFAULT gen_random_uuid()` | UUIDv4. Внутренний ключ для FK-связей. |
| `short_code` | `VARCHAR(16) NOT NULL UNIQUE` | User-facing короткий код (генерация — см. Секцию 4). |
| `long_url` | `TEXT NOT NULL` | Оригинальный URL. `TEXT`, потому что URL может быть длиннее 2 KB. |
| `long_url_hash` | `BYTEA NOT NULL` | SHA-256 от `long_url`, для уникальности по содержимому (идемпотентность). |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Время создания. |

Индексы:
- `PK (id)`.
- `UNIQUE (short_code)` — для редиректа.
- `UNIQUE (long_url_hash)` — для идемпотентного INSERT через `ON CONFLICT (long_url_hash)`.

> Почему `long_url_hash`, а не `UNIQUE (long_url)`: btree-индекс на `TEXT` лимитирован ~2700 байт; URL может быть длиннее. Хеш фикс-длины (32 байта) обходит проблему.

> `gen_random_uuid()` доступен в Postgres 13+ без дополнительных расширений.

### Таблица `click_stats` — агрегированная статистика

| Колонка | Тип | Описание |
|---|---|---|
| `url_id` | `UUID PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE` | FK на `urls.id`. PK = url_id, одна строка на URL. |
| `total_clicks` | `BIGINT NOT NULL DEFAULT 0` | Счётчик кликов. |
| `last_click_at` | `TIMESTAMPTZ` | Время последнего клика. NULL до первого клика. |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Время последнего апдейта (для отладки лагов consumer-а). |

Индексы: только PK.

> Намеренно **денормализованная агрегация**, а не таблица `click_events` со всеми кликами. Для MVP-аналитики (totalClicks, lastClickAt) этого хватает; сырые события живут в Kafka (с retention). Хранение каждого клика в БД — отдельная фича (Секция 8).

### Liquibase changelog layout

```
src/main/resources/db/changelog/
  db.changelog-master.yaml
  changes/
    0001-create-urls.yaml
    0002-create-click-stats.yaml
```

`db.changelog-master.yaml`:
```yaml
databaseChangeLog:
  - includeAll:
      path: changes/
      relativeToChangelogFile: true
```

Spring Boot подхватывает Liquibase автоматически через `spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.yaml`.

### JPA-сущности

Без `@OneToOne` — два отдельных репозитория, чтобы избежать lazy-loading и каскадных сюрпризов.

```java
@Entity @Table(name = "urls")
class UrlEntity {
  @Id
  @GeneratedValue
  @UuidGenerator(style = UuidGenerator.Style.RANDOM)   // Hibernate 6.2+, UUIDv4
  UUID id;

  @Column(name = "short_code", nullable = false, unique = true) String shortCode;
  @Column(name = "long_url", nullable = false) String longUrl;
  @Column(name = "long_url_hash", nullable = false) byte[] longUrlHash;
  @Column(name = "created_at", nullable = false) Instant createdAt;
}

@Entity @Table(name = "click_stats")
class ClickStatsEntity {
  @Id @Column(name = "url_id") UUID urlId;
  @Column(name = "total_clicks", nullable = false) long totalClicks;
  @Column(name = "last_click_at") Instant lastClickAt;
  @Column(name = "updated_at", nullable = false) Instant updatedAt;
}
```

### Зафиксированные решения

1. **`UUID` (v4) для `urls.id`** — простота, отсутствие зависимости от внешних библиотек/сервисов. Минус — фрагментация btree-индекса при высоких объёмах; не критично для учебного сервиса.
2. **Short-код не выводится из ID** (UUID → 22+ Base62-символов слишком длинно). Стратегия генерации — отдельная (Секция 4).
3. **Идемпотентность через `UNIQUE (long_url_hash)` + `ON CONFLICT`** — атомарно, без race-condition.
4. **`click_stats` как агрегат**; сырые события — в Kafka.
5. **Liquibase YAML changelog'и**.
6. **Без `@OneToOne`** между сущностями.

---

## 4. Генерация short-кода

### Подход: **Random Base62 + retry на коллизии**

- Генерируется 7-символьный код из алфавита `[0-9A-Za-z]` (62 символа → пространство 62⁷ ≈ 3.5·10¹²).
- Источник энтропии — `java.util.Random` (для MVP достаточно; криптостойкий `SecureRandom` — в Future features).
- INSERT с двумя `UNIQUE`-индексами:
  - `UNIQUE (long_url_hash)` — обеспечивает идемпотентность (один и тот же long URL → тот же short_code, даже при гонке).
  - `UNIQUE (short_code)` — защита от случайного дубля кода.
- При нарушении `UNIQUE (short_code)` — ретрай (максимум 5 попыток); при нарушении `UNIQUE (long_url_hash)` — возврат уже существующего маппинга.

### Алгоритм

```
function shorten(longUrl):
    hash = SHA-256(longUrl)
    existing = SELECT * FROM urls WHERE long_url_hash = hash
    if existing: return existing                       // идемпотентность

    for attempt in 1..5:
        code = randomBase62(length=7)
        try:
            INSERT INTO urls (id, short_code, long_url, long_url_hash, created_at)
            VALUES (gen_random_uuid(), code, longUrl, hash, now())
            ON CONFLICT (long_url_hash) DO NOTHING     // защита от race
            RETURNING *
            if inserted: return inserted
            else: return SELECT WHERE long_url_hash = hash   // другой поток успел
        catch UniqueViolation on short_code:
            continue                                    // редкая коллизия, ретрай
    throw ShortCodeGenerationException
```

### Рассмотренные альтернативы (не выбраны)

- **BIGSERIAL + Base62-кодирование** — монотонные коды, ноль коллизий, но раскрывает порядок и кол-во созданных ссылок (enumeration).
- **Truncated hash от `long_url`** — детерминированно, но детерминизм уже обеспечен `long_url_hash`, а коллизии 7-символьного префикса выше.

### Параметры (конфигурируемые через `application.yml`)

```yaml
urlshortener:
  shortcode:
    length: 7
    alphabet: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    max-collision-retries: 5
```

---

## 5. Kafka topics & stream processing

### Топики (один на тип события)

| Топик | Назначение | Партиций | Retention | Cleanup |
|---|---|---|---|---|
| `url.created.v1` | Публикуется при успешном shortening. Аудит/будущая аналитика. | 3 | 7 дней | `delete` |
| `url.clicked.v1` | Публикуется при каждом редиректе. Источник для агрегации в `click_stats`. | 6 | 7 дней | `delete` |
| `url.clicked.v1.DLT` | Dead-letter для непереварившихся `url.clicked.v1`. | 1 | 14 дней | `delete` |

Соглашения по неймингу: `<domain>.<event>.v<version>`. Версия в имени, эволюция схемы без breaking change.

### Ключ партиционирования

В обоих основных топиках **ключ = `shortCode`**. Это гарантирует, что все события одной короткой ссылки попадают в одну партицию → consumer обрабатывает их строго по порядку → нет гонок при UPSERT в `click_stats`.

### Схема событий (JSON)

**`UrlCreatedEvent` → `url.created.v1`:**
```json
{
  "eventId": "8f1c…-uuid",
  "occurredAt": "2026-05-22T10:15:30Z",
  "urlId": "9b2e…-uuid",
  "shortCode": "aB3xZ9",
  "longUrl": "https://example.com/..."
}
```

**`UrlClickedEvent` → `url.clicked.v1`:**
```json
{
  "eventId": "c4d2…-uuid",
  "occurredAt": "2026-05-22T11:42:01Z",
  "shortCode": "aB3xZ9",
  "urlId": "9b2e…-uuid"
}
```

Формат: **JSON через `JsonSerializer`/`JsonDeserializer` от spring-kafka** (без Schema Registry — для MVP избыточно; Avro/Protobuf + Schema Registry — Future feature).

### Producer

```java
@Component
class ClickEventPublisher {
  private final KafkaTemplate<String, UrlClickedEvent> kafka;

  void publish(UrlClickedEvent event) {
    kafka.send("url.clicked.v1", event.shortCode(), event);  // ключ = shortCode
  }
}
```

Конфиг продюсера:
```yaml
spring:
  kafka:
    producer:
      acks: all                  # надёжность
      enable-idempotence: true   # exactly-once в рамках одного продюсера
      retries: 5
      properties:
        max.in.flight.requests.per.connection: 5
```

> Publish клика делается **после успешного резолва** `shortCode → longUrl`, **до** возврата HTTP-ответа клиенту, но **асинхронно** (через `KafkaTemplate.send`, не ждём ack синхронно). При ошибке Kafka редирект всё равно отрабатывает — статистика потеряет одно событие, что приемлемо.

### Consumer

```java
@Component
class ClickStatsConsumer {
  @KafkaListener(
    topics = "url.clicked.v1",
    groupId = "click-stats-aggregator",
    concurrency = "3"
  )
  void onClick(UrlClickedEvent event) {
    clickStatsRepository.upsertIncrement(event.urlId(), event.occurredAt());
  }
}
```

Конфиг консьюмера:
```yaml
spring:
  kafka:
    consumer:
      group-id: click-stats-aggregator
      auto-offset-reset: earliest
      enable-auto-commit: false       # коммитим вручную после успешной обработки
      isolation-level: read_committed
    listener:
      ack-mode: record                 # коммит оффсета после каждого record
      type: single
```

### UPSERT в `click_stats`

Атомарный SQL (Postgres):
```sql
INSERT INTO click_stats (url_id, total_clicks, last_click_at, updated_at)
VALUES (:urlId, 1, :occurredAt, now())
ON CONFLICT (url_id) DO UPDATE SET
  total_clicks  = click_stats.total_clicks + 1,
  last_click_at = GREATEST(click_stats.last_click_at, EXCLUDED.last_click_at),
  updated_at    = now();
```

`GREATEST` защищает от out-of-order событий (хоть и маловероятно при партиционировании по `shortCode`).

### Обработка ошибок

- **DLQ-топик `url.clicked.v1.DLT`** — настраивается через `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`. Туда уходят сообщения, упавшие N раз (например, неконсистентный `urlId` без записи в `urls`).
- Backoff: `FixedBackOff(intervalMs=1000, maxAttempts=3)`.

### Семантика доставки

- Producer: **at-least-once** (с `enable.idempotence=true` дубли в рамках одного продюсера исключены; между перезапусками — возможны).
- Consumer: **at-least-once** + идемпотентный UPSERT → итог **effectively-exactly-once** для счётчика.

---

## 6. Observability & operability

### Логи

- **Формат**: JSON (один объект на строку), через `logback-spring.xml` + `logstash-logback-encoder`.
- **Обязательные поля**: `timestamp`, `level`, `logger`, `message`, `traceId`, `spanId`, `service=url-shortener`, `env`.
- **MDC**: `requestId` (из `X-Request-Id` или генерируется), `shortCode`, `urlId` (где применимо).
- **Уровни**: prod — `INFO`, dev — `DEBUG`. Никаких `System.out.println`.

### Метрики (Micrometer → Prometheus)

`spring-boot-starter-actuator` + `micrometer-registry-prometheus` → эндпоинт `/actuator/prometheus`.

Кастомные метрики:

| Метрика | Тип | Назначение |
|---|---|---|
| `urlshortener.urls.created.total` | Counter | Создано коротких ссылок. |
| `urlshortener.urls.created.duplicate.total` | Counter | Запросов, вернувших уже существующий маппинг. |
| `urlshortener.redirect.total{result}` | Counter (теги `hit`/`miss`) | Редиректы. |
| `urlshortener.redirect.latency` | Timer | Латентность редиректа (SLO-метрика). |
| `urlshortener.shortcode.collision.total` | Counter | Сколько раз сработал retry на коллизию short_code. |
| `urlshortener.kafka.click.publish.failed.total` | Counter | Не удалось опубликовать click-событие. |
| `urlshortener.kafka.click.consumed.total` | Counter | Обработано click-событий consumer-ом. |
| `urlshortener.kafka.dlq.total{topic}` | Counter | Сообщений отправлено в DLQ. |

Стандартные метрики из коробки: JVM, HTTP (`http.server.requests`), HikariCP, Kafka client.

### Трейсинг (OpenTelemetry)

- `micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp`.
- Экспорт через OTLP в **Jaeger** (поднимается в docker-compose).
- Auto-instrumentation для HTTP, JDBC, Kafka producer/consumer (распространение `traceId` через Kafka headers `traceparent`).
- В логах `traceId`/`spanId` подмешиваются через MDC.

### Health checks

`/actuator/health` через `spring-boot-starter-actuator`:
- `liveness`, `readiness` — Kubernetes-friendly probes.
- `db` — `DataSourceHealthIndicator`.
- `kafka` — `KafkaHealthIndicator` из spring-kafka.

```yaml
management:
  endpoint:
    health:
      probes:
        enabled: true
      show-details: when_authorized
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
```

### Graceful shutdown

```yaml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

Поведение при SIGTERM:
1. Spring перестаёт принимать новые HTTP-запросы, даёт текущим завершиться (до 30s).
2. Kafka listener-ы заканчивают обработку текущего record-а, коммитят оффсет, отписываются.
3. HikariCP и Kafka producer закрываются.

### `docker-compose.yml` для локальной разработки

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: urlshortener
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
    ports: ["5432:5432"]
    volumes: ["pgdata:/var/lib/postgresql/data"]

  kafka:
    image: apache/kafka:3.8.0      # KRaft mode, без Zookeeper
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    ports: ["9092:9092"]

  jaeger:
    image: jaegertracing/all-in-one:1.60
    ports: ["16686:16686", "4317:4317"]   # UI + OTLP gRPC

  prometheus:
    image: prom/prometheus:v2.54.0
    volumes: ["./ops/prometheus.yml:/etc/prometheus/prometheus.yml"]
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:11.2.0
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    ports: ["3000:3000"]

  app:
    build: .
    depends_on: [postgres, kafka]
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/urlshortener
      SPRING_DATASOURCE_USERNAME: app
      SPRING_DATASOURCE_PASSWORD: app
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      OTEL_EXPORTER_OTLP_ENDPOINT: http://jaeger:4317
    ports: ["8080:8080"]

volumes:
  pgdata:
```

### Конфигурация по 12-Factor

- Все настройки — через **env-переменные**.
- `application.yml` содержит только дефолты и плейсхолдеры (`${SPRING_DATASOURCE_URL:jdbc:postgresql://localhost:5432/urlshortener}`).
- Профили: `local`, `test`, `prod`.

---

## 7. Testing strategy

### Тестовая пирамида

```
       ┌────────┐
       │  E2E   │   ←  немного, по основным flow (1-2 теста)
       └────────┘
     ┌────────────┐
     │ Integration │  ←  Testcontainers: Postgres + Kafka
     └────────────┘
   ┌──────────────────┐
   │  Slice tests     │   ←  @WebMvcTest, @DataJpaTest, Kafka slice
   └──────────────────┘
 ┌────────────────────────┐
 │       Unit tests        │   ←  основная масса
 └────────────────────────┘
```

### 1. Unit (JUnit 5 + AssertJ + Mockito)

Без Spring-контекста. Покрытие:
- `ShortCodeGenerator` — длина, алфавит, поведение при коллизии (мок `Random`).
- `UrlService` — идемпотентность, обработка `UniqueViolation`.
- `RedirectService` — публикация события только при `hit`.
- `Base62Encoder`, DTO ↔ Entity мапперы.

### 2. Slice tests

- **`@WebMvcTest`** — контроллеры, сериализация DTO, статус-коды, `ErrorResponse` (RFC 7807). Сервисы — `@MockBean`.
- **`@DataJpaTest`** против Testcontainers Postgres:
  - UPSERT `click_stats` — две конкурентные вставки → `total_clicks=2`.
  - `UNIQUE (long_url_hash)` — повторный INSERT возвращает существующую запись.
- **Kafka slice** через `EmbeddedKafkaBroker` либо Testcontainers — продюсер → консьюмер.

### 3. Integration (Testcontainers: Postgres + Kafka)

Полный Spring-контекст (`@SpringBootTest`), реальные Postgres и Kafka через Testcontainers.

Шаблон базового класса:
```java
@SpringBootTest(webEnvironment = RANDOM_PORT)
@Testcontainers
abstract class IntegrationTestBase {
  @Container static PostgreSQLContainer<?> pg =
      new PostgreSQLContainer<>("postgres:16");

  @Container static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) {
    r.add("spring.datasource.url", pg::getJdbcUrl);
    r.add("spring.datasource.username", pg::getUsername);
    r.add("spring.datasource.password", pg::getPassword);
    r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }
}
```

Сценарии:
- **Happy path shortening**: `POST /api/v1/urls` → `201` → запись в `urls` → событие в `url.created.v1`.
- **Идемпотентность**: повторный `POST` с тем же URL → `200` + тот же `shortCode`, ровно одна запись в `urls`.
- **Редирект + статистика**: `GET /{shortCode}` → `302` → событие в `url.clicked.v1` → consumer инкрементит `click_stats` → `GET .../stats` возвращает свежее значение (с `Awaitility`).
- **404 на неизвестный код**: `GET /unknown` → `404` + `ErrorResponse`.
- **DLQ**: эмулируем падение consumer-а → сообщение в `url.clicked.v1.DLT`.
- **Graceful shutdown**: `SIGTERM` во время обработки → in-flight запросы завершаются.

Async-проверки — через `Awaitility`:
```java
await().atMost(Duration.ofSeconds(5))
    .untilAsserted(() ->
        assertThat(statsRepo.findByUrlId(urlId).getTotalClicks()).isEqualTo(1));
```

### 4. Contract tests (OpenAPI)

`swagger-request-validator` в `@WebMvcTest` — каждый ответ контроллера валидируется против спеки. Это страховка, что spec-first работает: реализация не уезжает от контракта.

### 5. E2E (минимум)

Один-два сценария поверх `docker-compose up` (`RestAssured`):
- shorten → redirect → stats.

### Технологии

| Инструмент | Назначение |
|---|---|
| JUnit 5 | Базовый раннер. |
| AssertJ | Fluent-ассерты. |
| Mockito | Моки. |
| Spring Boot Test | Slice / SpringBootTest. |
| Testcontainers | Postgres + Kafka в Docker. |
| Awaitility | Async-ассерты. |
| RestAssured | HTTP-вызовы в integration/e2e. |
| swagger-request-validator | OpenAPI-валидация ответов. |
| JaCoCo | Покрытие. |

### CI-pipeline (Gradle tasks)

- `./gradlew check` — unit + slice (быстро, < 1 минуты).
- `./gradlew integrationTest` — отдельный sourceSet, Testcontainers (1–3 мин).
- `./gradlew jacocoTestReport` — отчёт покрытия.
- `./gradlew openApiValidate` — линт OpenAPI спеки.

### Целевое покрытие

- **≥ 70% line / 60% branch** на сервисном слое.
- Контроллеры и мапперы покрываются естественно через slice-тесты.

---

## 8. Future features

Всё, что **сознательно отложено** за пределы MVP. В продакшен-готовом сервисе обязательно.

### Производительность

- **Кэширование `short_code → long_url`.** Redis или in-memory Caffeine между `RedirectService` и БД, т.к. отношение чтений к записям ~100:1. Cache-aside pattern: при miss дочитываем из БД и кладём в кэш. TTL ~1 час, invalidation при обновлении/удалении URL.
- **Read-replica для Postgres** — отделить чтение редиректов от записи новых URL.
- **Connection pool tuning** (HikariCP) и `EXPLAIN` query-планов под пиковую нагрузку.

### Безопасность

Полный пакет — отложен целиком в учебных целях, в продакшене обязателен:

- **Input validation.** Проверка схемы URL (только `http`/`https`), длины, RFC 3986. Защита от `javascript:`/`data:`/`file:` URI.
- **Anti-phishing/malware checks.** Чёрные списки доменов (Google Safe Browsing API, custom blocklist).
- **Rate limiting.** На `POST /api/v1/urls` и на редиректы. По IP и/или API-ключу. Bucket4j + Redis для distributed-варианта.
- **Защита от open redirect.** Запрет редиректа на internal/private IP-диапазоны (RFC 1918), на собственный домен в цикл.
- **HTTPS only.** TLS termination на API Gateway / Nginx; редирект `http → https`; HSTS-заголовок.
- **Authentication & authorization.** OAuth 2.0 / OIDC (Keycloak, Auth0). JWT для stateless auth. RBAC: owner ссылки vs admin.
- **Криптостойкая генерация `short_code`.** Замена `java.util.Random` на `SecureRandom` — защита от предсказуемости.
- **Secrets management.** Vault / AWS Secrets Manager / GCP Secret Manager — никаких креденшелов в env-файлах.
- **Least-privilege DB-роли.** Раздельные пользователи для миграций (DDL), приложения (DML без DDL), read-only (для аналитики).
- **API keys / signed URLs.** Защита публичных эндпоинтов от abuse.
- **Audit log.** Иммутабельный журнал создания/удаления ссылок, отдельный топик `url.audit.v1` с долгим retention.

### Аналитика и продуктовые фичи

- **Таблица сырых кликов `click_events`.** Отдельный consumer на `url.clicked.v1` пишет каждое событие (timestamp, IP, user-agent, referer, geo). Партиционирование по времени, retention-policy.
- **Real-time дашборды.** ksqlDB или Kafka Streams для агрегаций по окнам (`clicks per minute`, `top URLs last hour`).
- **Custom alias.** Возможность пользователю задать свой `shortCode` (`POST /api/v1/urls` с `customCode`).
- **Срок жизни ссылки (`expiresAt`).** TTL для URL, фоновый job чистит просроченные.
- **QR-код** для коротких ссылок (`GET /{shortCode}/qr`).
- **Bulk-shortening API** (`POST /api/v1/urls/bulk`).

### Эволюция Kafka

- **Schema Registry** (Confluent или Apicurio) + Avro/Protobuf вместо JSON — типобезопасность и контроль совместимости схем.
- **Transactional outbox pattern.** Гарантия «БД-запись + Kafka-событие» атомарно: пишем событие в таблицу `outbox` в одной транзакции с `urls`, отдельный publisher вычитывает и шлёт в Kafka. Закрывает дыру с потерей `UrlClickedEvent` при сбое publish.
- **Compact topic для `url.created.v1`** с ключом `shortCode` — даёт быструю реконструкцию состояния.

### Масштабирование

- **Horizontal scaling** API-слоя (stateless процессы за балансировщиком, k8s HPA по CPU/RPS).
- **Горизонтальное масштабирование consumer-ов** через увеличение партиций `url.clicked.v1` и подъём дополнительных инстансов в consumer group.
- **Sharded Postgres / CockroachDB** на больших объёмах.
- **CDN перед редиректом.** Edge-кэширование маппингов на CloudFlare Workers / Fastly — миллисекундная латентность, разгрузка origin.

### Operability

- **Distributed tracing в проде** — Tempo / Honeycomb / Datadog APM вместо одиночного Jaeger.
- **Алерты в Grafana / PagerDuty** на ключевые SLO: redirect p99 latency, error rate, consumer lag, DLQ growth rate.
- **Chaos engineering** — `Toxiproxy` в integration-тестах для проверки поведения при деградации Kafka/Postgres.
- **Multi-region deploy** с асинхронной репликацией БД и Kafka MirrorMaker.

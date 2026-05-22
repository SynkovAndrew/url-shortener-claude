# URL Shortener — План реализации

**Цель:** построить учебный URL-shortener на Java 21 + Spring Boot 3.5 + Apache Kafka (KRaft) + PostgreSQL 16 со spec-first OpenAPI, идемпотентным shortening-ом, асинхронной аналитикой кликов через Kafka и observability (Micrometer + OpenTelemetry).

## Процесс выполнения

**Изучи контекст:**
- Прочитай `docs/results/PROJECT-DESIGN.md` — это источник правды по архитектуре, схемам, конфигам.
- Найди первую незакрытую фичу в этом плане (без `✅` в заголовке).

**Выполни фичу:**
- Выполни все шаги фичи по порядку.
- **Тесты обязательны для каждой фичи** (юниты + интеграционные где указано).
- Если по ходу всплывает развилка, не описанная в дизайне, — кратко опиши пользователю и жди подтверждения.

**Подведи итог:**
- Напиши короткий итог: что сделал, что работает, есть ли проблемы.
- Жди подтверждения пользователя.
- После подтверждения: отметь шаги `- [x]` и поставь `✅` в заголовке фичи.

## Принципы

- **DRY** — не дублируй код, выноси общее (например, базовый класс для интеграционных тестов).
- **YAGNI** — не добавляй то, что не описано в дизайне (всё security/caching/прочее — в `Future features` дизайна).
- **Версии из BOM** — фиксируй только `spring-boot.version`; junit/mockito/assertj/awaitility подтягиваются из `spring-boot-dependencies`.
- **`spring.jpa.open-in-view=false`** — выключи явно, дефолт ловушка.
- **Не вкоммитывай сгенерированный код** (`build/generated/...`).

## Ключевые версии (зафиксированы на май 2026)

- Spring Boot **3.5.x** (последняя 3.x; 4.x пока избыточен).
- openapi-generator-gradle-plugin **7.22.0** (7.23 — breaking).
- Testcontainers **1.21.x** (используй `org.testcontainers.kafka.KafkaContainer`, **не** `containers.KafkaContainer`).
- Kafka image: **`apache/kafka:3.8.0`** (KRaft по умолчанию).
- Liquibase **4.31.x**, Hibernate **6.6.x** (через BOM Boot).
- micrometer-tracing-bridge-otel **1.4.x**, opentelemetry-exporter-otlp **1.43.x** (BOM-managed).
- logstash-logback-encoder **8.0** (для Boot 3.5 / Logback 1.5) — но MVP может обойтись встроенным `logging.structured.format.console=logstash`.
- swagger-request-validator-mockmvc **2.46.0**.

---

### Фича 1: Bootstrap проекта и docker-compose

**Цель:** проект собирается через `./gradlew build`, docker-compose поднимает Postgres + Kafka + Jaeger + Prometheus + Grafana, Spring Boot стартует и отдаёт `/actuator/health` = UP.

**Файлы:**
- Создать: `settings.gradle.kts`, `build.gradle.kts`, `gradle.properties`, `.gitignore`, `gradlew` (`gradle wrapper`).
- Создать: `src/main/java/com/example/urlshortener/UrlShortenerApplication.java` — пустой `@SpringBootApplication`.
- Создать: `src/main/resources/application.yml` (placeholders с env-переменными, профили `local|test|prod`).
- Создать: `docker-compose.yml` (postgres, kafka в KRaft, jaeger, prometheus, grafana — копия из `PROJECT-DESIGN.md` Секция 6).
- Создать: `ops/prometheus.yml` (минимальный scrape config для `app:8080/actuator/prometheus`).
- Тесты: `src/test/java/com/example/urlshortener/UrlShortenerApplicationTests.java` (`@SpringBootTest` smoke `contextLoads()`).

**Шаги:**
- [ ] `gradle wrapper --gradle-version 8.10` — сгенерировать обёртку.
- [ ] В `build.gradle.kts` подключить плагины `java`, `org.springframework.boot 3.5.x`, `io.spring.dependency-management`. Зависимости: `spring-boot-starter-web`, `-data-jpa`, `-actuator`, `spring-kafka`, драйвер `org.postgresql:postgresql`, `liquibase-core`. `JavaVersion.VERSION_21`.
- [ ] Завести sourceSet `integrationTest` (отдельная папка `src/integrationTest/java`, отдельная gradle task `integrationTest`).
- [ ] Написать `application.yml` с placeholder'ами для DataSource / Kafka из env (`${SPRING_DATASOURCE_URL:...}`).
- [ ] Написать `docker-compose.yml` и `ops/prometheus.yml`.
- [ ] `.gitignore`: `build/`, `.gradle/`, `*.log`, `out/`.

**Проверка:**
- [ ] `./gradlew build` — BUILD SUCCESSFUL, контекст поднимается в smoke-тесте.
- [ ] `docker compose up -d postgres kafka` — оба контейнера в `healthy`/`running`.
- [ ] `docker compose down` — без ошибок.

---

### Фича 2: Liquibase миграции + JPA-сущности + репозитории

**Цель:** при старте приложение применяет миграции в реальный Postgres; CRUD-репозитории `urls` и `click_stats` работают (проверено через Testcontainers).

**Файлы:**
- Создать: `src/main/resources/db/changelog/db.changelog-master.yaml` (`includeAll: changes/`).
- Создать: `src/main/resources/db/changelog/changes/0001-create-urls.yaml`, `0002-create-click-stats.yaml` (схема из дизайна, Секция 3).
- Создать: `src/main/java/com/example/urlshortener/persistence/UrlEntity.java`, `ClickStatsEntity.java`.
- Создать: `src/main/java/com/example/urlshortener/persistence/UrlRepository.java`, `ClickStatsRepository.java` (Spring Data JPA).
- Тесты: `src/integrationTest/java/.../persistence/UrlRepositoryIntegrationTest.java`, `ClickStatsRepositoryIntegrationTest.java` (`@DataJpaTest` + Testcontainers Postgres).
- Тесты: общий базовый класс `src/integrationTest/java/.../support/PostgresIntegrationTestBase.java`.

**Шаги:**
- [ ] Добавить testcontainers: `org.testcontainers:postgresql`, `:junit-jupiter`. BOM из `testcontainers-bom`.
- [ ] Описать changelog'и для `urls` (PK uuid, unique `short_code`, unique `long_url_hash`) и `click_stats` (PK = `url_id` FK ON DELETE CASCADE). Tип колонки `type: uuid`.
- [ ] `UrlEntity`: `@Id @GeneratedValue @UuidGenerator(style=RANDOM) UUID id;`, поля как в дизайне (`@Column(name=...)`).
- [ ] `ClickStatsEntity`: `@Id UUID urlId;` (без `@GeneratedValue` — генерируется из FK).
- [ ] Метод `ClickStatsRepository.upsertIncrement(UUID urlId, Instant occurredAt)` — нативный SQL UPSERT из дизайна (Секция 5).
- [ ] Метод `UrlRepository.findByLongUrlHash(byte[] hash)`, `findByShortCode(String)`.
- [ ] Базовый класс `PostgresIntegrationTestBase`: `@Testcontainers`, статический `PostgreSQLContainer<>("postgres:16")`, `@DynamicPropertySource`. Включить `spring.jpa.open-in-view=false`, `spring.jpa.hibernate.ddl-auto=validate`.
- [ ] Тест `UrlRepositoryIntegrationTest`: insert → findByShortCode возвращает; UNIQUE на `long_url_hash` бросает `DataIntegrityViolationException`.
- [ ] Тест `ClickStatsRepositoryIntegrationTest`: вызвать `upsertIncrement` дважды → `total_clicks=2`, `last_click_at` = max.

**Проверка:**
- [ ] `./gradlew integrationTest --tests "*RepositoryIntegrationTest"` — все тесты зелёные, миграции применяются автоматически.

---

### Фича 3: OpenAPI скелет + кодоген

**Цель:** YAML-спека с минимальным эндпоинтом (`GET /actuator/health` не считаем — нужен пустой `paths: {}` или один dummy-эндпоинт), плагин openapi-generator генерирует Spring 6 / Jakarta EE интерфейсы, проект компилируется.

**Файлы:**
- Создать: `src/main/resources/openapi/url-shortener.yaml` (минимальный каркас: `info`, `servers`, `tags`, `paths: {}`, `components.schemas: { ErrorResponse }` (RFC 7807)).
- Изменить: `build.gradle.kts` — плагин `org.openapi.generator 7.22.0`, конфиг `openApiGenerate`, `sourceSets.main.java.srcDir(...)`, `compileJava.dependsOn("openApiGenerate")`.

**Шаги:**
- [ ] Добавить плагин `org.openapi.generator` версии `7.22.0`.
- [ ] `openApiGenerate` configOptions: `interfaceOnly=true`, `useSpringBoot3=true`, `useJakartaEe=true`, `useTags=true`, `dateLibrary=java8`, `skipDefaultInterface=true`, `openApiNullable=false`.
- [ ] `apiPackage=com.example.urlshortener.api`, `modelPackage=com.example.urlshortener.api.model`.
- [ ] Положить YAML с заглушкой `paths: {}` (валидная OpenAPI 3.0.3) и схемой `ErrorResponse`.
- [ ] Добавить `./gradlew openApiValidate` в CI-набор (если плагин его не подключает — настроить).
- [ ] Убедиться, что генерируется DTO `ErrorResponse` в `build/generated/...`.

**Проверка:**
- [ ] `./gradlew openApiValidate` — спека валидна.
- [ ] `./gradlew openApiGenerate compileJava` — генерируется и компилируется, `build/generated/openapi/.../model/ErrorResponse.java` существует.

---

### Фича 4: ShortCodeGenerator (Base62 + retry)

**Цель:** утилита генерирует 7-символьные коды из алфавита `[0-9A-Za-z]`; конфиг параметров через `application.yml`; retry на коллизию проверяется юнит-тестом с замоканным `Random`.

**Файлы:**
- Создать: `src/main/java/com/example/urlshortener/service/shortcode/Base62Encoder.java`.
- Создать: `src/main/java/com/example/urlshortener/service/shortcode/ShortCodeGenerator.java`.
- Создать: `src/main/java/com/example/urlshortener/config/ShortCodeProperties.java` (`@ConfigurationProperties("urlshortener.shortcode")`).
- Изменить: `application.yml` — добавить блок `urlshortener.shortcode.{length,alphabet,max-collision-retries}`.
- Тесты: `src/test/java/.../service/shortcode/ShortCodeGeneratorTest.java`, `Base62EncoderTest.java`.

**Шаги:**
- [ ] `Base62Encoder` — без состояния, метод `String encode(byte[] randomBytes, int length, String alphabet)`.
- [ ] `ShortCodeGenerator` — принимает `Random`/`Supplier<String>` через конструктор (для тестируемости).
- [ ] Включить `@EnableConfigurationProperties(ShortCodeProperties.class)` в одной из `@Configuration`-классов (создай `AppConfig`).
- [ ] Тесты: длина = 7, символы только из алфавита, при детерминированном `Random` коды воспроизводимы.

**Проверка:**
- [ ] `./gradlew test --tests "*shortcode*"` — все юнит-тесты проходят.

---

### Фича 5: POST /api/v1/urls (создание + идемпотентность)

**Цель:** эндпоинт принимает `longUrl`, сохраняет в БД с идемпотентным INSERT (через `UNIQUE long_url_hash`), возвращает `201 Created` + `ShortenedUrl` (или `200 OK` если уже существует). Интеграционный тест через Testcontainers.

**Файлы:**
- Изменить: `src/main/resources/openapi/url-shortener.yaml` — добавить `POST /api/v1/urls`, схемы `CreateShortUrlRequest`, `ShortenedUrl`.
- Создать: `src/main/java/.../service/UrlService.java` (метод `shorten(longUrl)`).
- Создать: `src/main/java/.../api/ShortenerController.java implements UrlsApi`.
- Создать: `src/main/java/.../service/HashingUtil.java` (SHA-256 хеш `byte[]` от `longUrl`).
- Создать: `src/main/java/.../service/UrlMapper.java` (Entity ↔ DTO).
- Тесты: `src/test/java/.../service/UrlServiceTest.java` (unit, моки репо), `src/integrationTest/java/.../shortener/ShortenIntegrationTest.java`.

**Шаги:**
- [ ] Дополнить OpenAPI: тело запроса, ответы 200/201 (оба с `ShortenedUrl`), ошибка 400 с `ErrorResponse`.
- [ ] Реализовать `UrlService.shorten`: SHA-256 → `findByLongUrlHash` → если есть, вернуть существующий с флагом `created=false`; иначе цикл retry (max из конфига) на генерацию `shortCode` + insert с `try/catch DataIntegrityViolationException`.
- [ ] При нарушении уникальности `long_url_hash` (race) — повторный SELECT и возврат существующего.
- [ ] При исчерпании ретраев — кастомное `ShortCodeGenerationException`.
- [ ] Контроллер: вызывает сервис, возвращает `ResponseEntity` 201 (`Location: /{shortCode}`) или 200 в зависимости от флага.
- [ ] Базовый класс `IntegrationTestBase` (Postgres + Kafka контейнеры) — пока без Kafka, дополним в Фиче 6.
- [ ] Интеграционный тест: первый POST → 201; второй POST с тем же URL → 200, тот же `shortCode`; в БД ровно одна запись.

**Проверка:**
- [ ] `./gradlew check integrationTest --tests "ShortenIntegrationTest"` — зелёные.
- [ ] `./gradlew openApiValidate` — спека по-прежнему валидна.

---

### Фича 6: Kafka producer для `url.created.v1`

**Цель:** при успешном создании URL в Kafka-топик `url.created.v1` уходит `UrlCreatedEvent` (ключ = `shortCode`). Интеграционный тест через Testcontainers Kafka подтверждает доставку.

**Файлы:**
- Создать: `src/main/java/.../kafka/event/UrlCreatedEvent.java` (record).
- Создать: `src/main/java/.../kafka/UrlCreatedPublisher.java`.
- Создать: `src/main/java/.../config/KafkaTopicsConfig.java` (`NewTopic` beans: `url.created.v1`, `url.clicked.v1`, `url.clicked.v1.DLT`).
- Создать: `src/main/java/.../config/KafkaProducerConfig.java` (KafkaTemplate, `JsonSerializer`, `acks=all`, `enable.idempotence=true`).
- Изменить: `UrlService` — вызов `urlCreatedPublisher.publish(...)` после успешного insert.
- Изменить: `application.yml` — `spring.kafka.producer.*` (acks=all, idempotence, retries=5).
- Изменить: базовый класс интеграционных тестов — добавить `KafkaContainer` (импорт из `org.testcontainers.kafka`, image `apache/kafka:3.8.0`).
- Тесты: `src/integrationTest/java/.../kafka/UrlCreatedKafkaIntegrationTest.java` (POST → ждём событие в топике с `KafkaTestUtils.getRecords` / `@KafkaListener`-test consumer + `Awaitility`).

**Шаги:**
- [ ] Добавить зависимости: `org.testcontainers:kafka`, `org.springframework.kafka:spring-kafka-test`, `awaitility`.
- [ ] Объявить `NewTopic` beans с партициями: `url.created.v1` (3), `url.clicked.v1` (6), `url.clicked.v1.DLT` (1).
- [ ] `UrlCreatedPublisher.publish` — асинхронно `kafkaTemplate.send(topic, event.shortCode(), event)`. Метрика `urlshortener.urls.created.total` (Фича 12 — пока можно без неё).
- [ ] `IntegrationTestBase` дополнить: статический `KafkaContainer` (`apache/kafka:3.8.0`), `@DynamicPropertySource` подкидывает `spring.kafka.bootstrap-servers`.
- [ ] Интеграционный тест: POST → собираем records из `url.created.v1`, валидируем `shortCode`, `longUrl`, `urlId`.

**Проверка:**
- [ ] `./gradlew integrationTest --tests "*UrlCreated*"` — зелёные, событие в топике.

---

### Фича 7: GET /api/v1/urls/{shortCode}

**Цель:** эндпоинт возвращает `ShortenedUrl` по короткому коду; 404 + `ErrorResponse` (RFC 7807), если кода нет.

**Файлы:**
- Изменить: `openapi/url-shortener.yaml` — добавить `GET /api/v1/urls/{shortCode}`, ответы 200/404.
- Изменить: `UrlService` — метод `findByShortCode(String)`.
- Изменить: `ShortenerController` — реализовать `getShortUrl`.
- Создать: `src/main/java/.../api/exception/ShortCodeNotFoundException.java`.
- Создать: `src/main/java/.../api/exception/GlobalExceptionHandler.java` (`@ControllerAdvice`, маппит `ShortCodeNotFoundException` → 404 + RFC 7807 `ErrorResponse`).
- Тесты: `src/integrationTest/java/.../shortener/GetUrlIntegrationTest.java`, `src/test/java/.../api/exception/GlobalExceptionHandlerTest.java` (`@WebMvcTest`).

**Шаги:**
- [ ] Описать `GET /api/v1/urls/{shortCode}` в OpenAPI с `operationId=getShortUrl`.
- [ ] Сервис + контроллер.
- [ ] `GlobalExceptionHandler` строит `ErrorResponse` с полями `type`, `title`, `status`, `detail`, `instance`.
- [ ] Интеграционные тесты: 200 (после shortening), 404 на unknown.
- [ ] Slice-тест: моки сервиса, проверка JSON ответа 404 на соответствие схеме `ErrorResponse`.

**Проверка:**
- [ ] `./gradlew check integrationTest --tests "GetUrlIntegrationTest"` — зелёные.

---

### Фича 8: Редирект `GET /{shortCode}` + publish `url.clicked.v1`

**Цель:** запрос на `GET /{shortCode}` возвращает `302 Found` с `Location: <longUrl>`, асинхронно публикует `UrlClickedEvent` в `url.clicked.v1` (ключ = `shortCode`). Промах — 404.

**Файлы:**
- Изменить: `openapi/url-shortener.yaml` — `GET /{shortCode}`, ответ 302 (`Location` header), 404.
- Создать: `src/main/java/.../api/RedirectController.java implements RedirectApi`.
- Создать: `src/main/java/.../service/RedirectService.java`.
- Создать: `src/main/java/.../kafka/event/UrlClickedEvent.java` (record).
- Создать: `src/main/java/.../kafka/ClickEventPublisher.java`.
- Тесты: `src/test/java/.../service/RedirectServiceTest.java` (unit), `src/integrationTest/java/.../redirect/RedirectIntegrationTest.java`.

**Шаги:**
- [ ] Описать `GET /{shortCode}` в OpenAPI (operationId `redirectToLongUrl`, response 302 с `Location`-header).
- [ ] `RedirectController` возвращает `ResponseEntity.status(HttpStatus.FOUND).location(URI.create(longUrl)).build()`.
- [ ] `RedirectService.resolve(shortCode)`: SELECT + `clickEventPublisher.publish(...)` **только при hit**; при miss — `ShortCodeNotFoundException`.
- [ ] Publish — async через `KafkaTemplate.send`, не ждём ack (см. дизайн Секция 5).
- [ ] Unit-тест: при miss publish **не вызывается**.
- [ ] Интеграционный тест: shorten → GET `/{shortCode}` → 302 + correct Location, событие появляется в `url.clicked.v1` (Awaitility).

**Проверка:**
- [ ] `./gradlew check integrationTest --tests "Redirect*"` — зелёные.

---

### Фича 9: Kafka consumer + UPSERT в `click_stats`

**Цель:** `ClickStatsConsumer` слушает `url.clicked.v1`, выполняет атомарный UPSERT в `click_stats`. Партиционирование по `shortCode` гарантирует порядок.

**Файлы:**
- Создать: `src/main/java/.../kafka/ClickStatsConsumer.java`.
- Создать: `src/main/java/.../config/KafkaConsumerConfig.java` (JsonDeserializer + trusted packages, manual ack, single record listener, concurrency=3, isolation_level=read_committed).
- Изменить: `application.yml` — `spring.kafka.consumer.*`, `spring.kafka.listener.ack-mode=record`.
- Изменить: `ClickStatsRepository.upsertIncrement` (если ещё не реализован — реализовать через `@Modifying @Query`).
- Тесты: `src/integrationTest/java/.../kafka/ClickStatsConsumerIntegrationTest.java`.

**Шаги:**
- [ ] Конфиг consumer'а: `group-id=click-stats-aggregator`, `auto-offset-reset=earliest`, `enable-auto-commit=false`, deserializer JSON с типом `UrlClickedEvent`.
- [ ] `@KafkaListener(topics="url.clicked.v1", concurrency="3")` — вызывает `clickStatsRepository.upsertIncrement(urlId, occurredAt)`.
- [ ] Интеграционный тест: shorten → 3 раза GET `/{shortCode}` → Awaitility: `click_stats.total_clicks == 3`, `last_click_at` обновлено.
- [ ] Дополнительный тест: 2 параллельных события для одной ссылки → итого `total_clicks == 2` (UPSERT детерминирован).

**Проверка:**
- [ ] `./gradlew integrationTest --tests "ClickStatsConsumer*"` — зелёные, Awaitility ждёт ≤ 5s.

---

### Фича 10: GET /api/v1/urls/{shortCode}/stats

**Цель:** эндпоинт возвращает `UrlStats { shortCode, totalClicks, lastClickAt }`. 404 если кода нет; если `click_stats` пустой — возвращает `totalClicks=0`, `lastClickAt=null`.

**Файлы:**
- Изменить: `openapi/url-shortener.yaml` — `GET /api/v1/urls/{shortCode}/stats`, схема `UrlStats`.
- Изменить: `UrlService` (или новый `StatsService`) — метод `getStats(shortCode)`.
- Изменить: `ShortenerController` — реализовать `getUrlStats`.
- Тесты: `src/integrationTest/java/.../shortener/StatsIntegrationTest.java`.

**Шаги:**
- [ ] Спека + DTO `UrlStats` (с `lastClickAt: nullable: true`).
- [ ] Сервис: `findByShortCode` → если нет, `ShortCodeNotFoundException`; иначе `clickStatsRepository.findByUrlId` + fallback на `total_clicks=0`.
- [ ] Интеграционный тест: shorten → GET stats → `totalClicks=0`; затем GET `/{shortCode}` 3 раза → Awaitility → GET stats → `totalClicks=3`.
- [ ] Тест на 404 для unknown shortCode.

**Проверка:**
- [ ] `./gradlew integrationTest --tests "Stats*"` — зелёные.

---

### Фича 11: Глобальные ошибки (RFC 7807) + Kafka DLT

**Цель:** все ошибки API уходят в формате RFC 7807; на consumer'е настроен `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` → битые сообщения попадают в `url.clicked.v1.DLT`; `DeserializationException` помечена как non-retryable.

**Файлы:**
- Изменить: `GlobalExceptionHandler` — добавить хендлеры на `MethodArgumentNotValidException` (400), `HttpMessageNotReadableException` (400), `Exception` fallback (500).
- Создать: `src/main/java/.../config/KafkaErrorHandlerConfig.java` (`DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `FixedBackOff(1000,2)`).
- Создать: `src/main/java/.../kafka/DlqKafkaTemplateConfig.java` — отдельный `KafkaTemplate<byte[],byte[]>` (`ByteArraySerializer`) для DLT recoverer (рекомендация из research).
- Тесты: `src/test/java/.../api/exception/GlobalExceptionHandlerTest.java` (slice-тест на JSON ошибок). `src/integrationTest/java/.../kafka/KafkaDlqIntegrationTest.java` — посылаем мусорный JSON в `url.clicked.v1`, проверяем что появилось в `url.clicked.v1.DLT`.
- Изменить: добавить contract-тест `src/test/java/.../api/contract/ApiContractTest.java` через `swagger-request-validator-mockmvc` — каждый ответ контроллера валидируется против OpenAPI спеки.

**Шаги:**
- [ ] Добавить зависимость `com.atlassian.oai:swagger-request-validator-mockmvc:2.46.0` (test).
- [ ] `DefaultErrorHandler`: `addNotRetryableExceptions(IllegalArgumentException.class, DeserializationException.class)`.
- [ ] DLT recoverer: маппит топик → `topic + ".DLT"`, та же партиция.
- [ ] `NewTopic` для `.DLT` (1 партиция) уже создан в Фиче 6 — проверить.
- [ ] Тест DLQ: отправляем в `url.clicked.v1` raw bytes невалидного JSON → ждём record в `.DLT` через Awaitility.
- [ ] Contract-тест: вызовы `mockMvc.perform(...).andExpect(openApi().isValid(...))` для каждого endpoint'а.

**Проверка:**
- [ ] `./gradlew check integrationTest --tests "GlobalExceptionHandlerTest,KafkaDlqIntegrationTest,ApiContractTest"` — зелёные.

---

### Фича 12: Observability + graceful shutdown + smoke E2E

**Цель:** JSON-логи с MDC; Micrometer-метрики на Prometheus-эндпоинте; OpenTelemetry трейсинг с пропагацией через Kafka headers; health/readiness/liveness; graceful shutdown за ≤30s; E2E-сценарий поверх `docker compose up`.

**Файлы:**
- Изменить: `application.yml` — `management.endpoints.web.exposure.include`, `management.endpoint.health.probes.enabled=true`, `server.shutdown=graceful`, `spring.lifecycle.timeout-per-shutdown-phase=30s`, `management.tracing.sampling.probability=1.0`, `logging.structured.format.console=logstash`.
- Изменить: `build.gradle.kts` — добавить `micrometer-registry-prometheus`, `micrometer-tracing-bridge-otel`, `opentelemetry-exporter-otlp` (версии — из BOM).
- Создать: `src/main/java/.../config/MetricsConfig.java` — `Counter`/`Timer` beans: `urlshortener.urls.created.total`, `urlshortener.urls.created.duplicate.total`, `urlshortener.redirect.total{result}`, `urlshortener.redirect.latency`, `urlshortener.shortcode.collision.total`, `urlshortener.kafka.click.publish.failed.total`, `urlshortener.kafka.click.consumed.total`, `urlshortener.kafka.dlq.total{topic}`.
- Изменить: соответствующие сервисы / publisher'ы / consumer — увеличивают метрики.
- Создать: `src/main/java/.../api/RequestIdFilter.java` — кладёт `X-Request-Id` (или генерит UUID) в MDC.
- Тесты: `src/integrationTest/java/.../e2e/EndToEndSmokeTest.java` — RestAssured, через `IntegrationTestBase`: POST → GET /{code} → GET /stats. Awaitility на consumer.

**Шаги:**
- [ ] Подключить Prometheus + OTel exporter.
- [ ] Зарегистрировать кастомные метрики, расставить вызовы (`counter.increment()` в нужных точках).
- [ ] `RequestIdFilter`: order high, кладёт `requestId` в MDC, чистит в `finally`.
- [ ] Проверить эндпоинт `/actuator/prometheus` — содержит кастомные метрики.
- [ ] E2E-тест: полный happy-path через RestAssured.
- [ ] (Опционально) ручной smoke — `docker compose up`, `curl -X POST http://localhost:8080/api/v1/urls -d '{"longUrl":"https://example.com"}'` → редирект → stats.

**Проверка:**
- [ ] `./gradlew check integrationTest` — все тесты зелёные.
- [ ] Ручной запуск: `docker compose up -d` → `curl -sf http://localhost:8080/actuator/health | jq '.status'` → `"UP"`.
- [ ] `curl -s http://localhost:8080/actuator/prometheus | grep urlshortener_` — кастомные метрики видны.
- [ ] В Jaeger UI (`http://localhost:16686`) после нескольких запросов виден сервис `url-shortener` со span'ами HTTP + JDBC + Kafka.

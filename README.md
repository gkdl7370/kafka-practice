# kafka-practice

[![Java](https://img.shields.io/badge/Java_17-ED8B00?style=flat&logo=openjdk&logoColor=white)]()
[![Spring Boot](https://img.shields.io/badge/Spring_Boot_3.2-6DB33F?style=flat&logo=spring-boot&logoColor=white)]()
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)]()

**Spring Boot 기반 Kafka Producer/Consumer 실습 — Redis Pub/Sub와의 트레이드오프 비교 목적으로 구현**

---

## 구현 배경

기존 프로젝트(MassFlux-Gateway)에서 Redis Pub/Sub로 비동기 이벤트 처리를 구현했습니다.  
Redis 선택 당시 Kafka의 메시지 내구성·재처리 강점을 인지했지만, 폐쇄망 환경의 브로커 클러스터 운영 부담으로 Redis를 선택했습니다.

이 프로젝트는 **Kafka를 직접 구현하고 Redis Pub/Sub와 트레이드오프를 체득하기 위해** 만들었습니다.

---

## 시스템 구조

```
REST API 요청
     │
     ▼
SensorController
     │ POST /api/sensor/{deviceId}
     ▼
SensorProducer ──────────────────────────────────────────►  Kafka Broker
  KafkaTemplate.send(topic, key, message)                   sensor-events Topic
                                                            ├── partition 0
                                                            ├── partition 1
                                                            └── partition 2
                                                                    │
                                                                    ▼
                                                            SensorConsumer
                                                            @KafkaListener
                                                            processData()
```

---

## 핵심 구현

### 1. Producer — 메시지 전송 + 결과 비동기 처리

```java
public void send(String deviceId, String message) {
    kafkaTemplate.send(TOPIC, deviceId, message)
        .whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[Producer] 전송 성공 - deviceId: {}, partition: {}, offset: {}",
                    deviceId,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            } else {
                log.error("[Producer] 전송 실패 - deviceId: {}, 원인: {}", deviceId, ex.getMessage());
            }
        });
}
```

**설계 포인트:**
- `deviceId`를 Key로 사용 → 동일 디바이스 메시지가 항상 같은 partition으로 라우팅 (순서 보장)
- `whenComplete`로 비동기 전송 결과 처리 → 전송 실패 시 로그로 추적 가능

### 2. Consumer — partition·offset 기반 메시지 수신

```java
@KafkaListener(topics = "sensor-events", groupId = "sensor-group")
public void consume(
        @Payload String message,
        @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset) {

    log.info("[Consumer] 수신 - deviceId: {}, message: {}, partition: {}, offset: {}",
        key, message, partition, offset);
}
```

**설계 포인트:**
- `required = false` → Key 없는 메시지도 처리 가능 (CLI 테스트 대응)
- partition, offset 로깅 → 메시지 위치 추적 및 재처리 기준점 파악 가능
- `groupId = sensor-group` → Consumer Group으로 수평 확장 시 partition 자동 분배

### 3. Topic 구성

```bash
kafka-topics --create \
  --topic sensor-events \
  --partitions 3 \          # 병렬 처리를 위한 partition 3개
  --replication-factor 1    # 단일 브로커 환경 (로컬)
```

**partition 3개로 구성한 이유:**
- Consumer를 3개까지 수평 확장 시 각 Consumer가 partition 1개씩 담당
- partition 수 = Consumer 병렬 처리 한계선

---

## 실행 방법

### 1. Kafka 브로커 실행

```bash
docker-compose up -d
```

### 2. Topic 생성

```bash
docker exec kafka kafka-topics \
  --create \
  --topic sensor-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Spring Boot 실행

```bash
./mvnw spring-boot:run
```

### 4. 메시지 전송 테스트

```bash
# REST API로 Producer 호출
curl -X POST http://localhost:8080/api/sensor/device-001 \
  -H "Content-Type: application/json" \
  -d '"temperature:36.5"'
```

**예상 로그:**
```
[Producer] 전송 성공 - deviceId: device-001, partition: 0, offset: 0
[Consumer] 수신  - deviceId: device-001, message: temperature:36.5, partition: 0, offset: 0
[Consumer] 처리 완료 - deviceId: device-001
```

---

## Redis Pub/Sub vs Kafka 트레이드오프

| 항목 | Redis Pub/Sub | Kafka |
|------|--------------|-------|
| 메시지 내구성 | 없음 (메모리, 수신자 없으면 유실) | 있음 (디스크 저장, 보존 기간 설정) |
| 재처리 | 불가 | 가능 (offset 기반) |
| 순서 보장 | 보장 안 됨 | partition 내 순서 보장 |
| 수평 확장 | 제한적 | Consumer Group으로 자연스러운 확장 |
| 운영 복잡도 | 낮음 (단일 프로세스) | 높음 (브로커 클러스터 필요) |
| 저지연 | 매우 낮음 (메모리) | 상대적으로 높음 |
| 적합한 상황 | 실시간 알림, 단순 이벤트 전달 | 대용량 로그, 이벤트 소싱, 재처리 필요 시 |

**MassFlux-Gateway에서 Redis를 선택한 이유:**
폐쇄망 환경의 브로커 클러스터 구성·운영 부담 > 요구사항(실시간 수위 이벤트 처리)에서 Kafka의 이점.  
저지연과 구조 단순성이 해당 도메인에 더 적합했고, 데이터 유실 리스크는 DB 선적재 + 배치 재처리로 보완해 정합성을 확보했습니다.

---

## 기술 스택

- Java 17
- Spring Boot 3.2
- Spring Kafka
- Apache Kafka 7.4.0 (Confluent)
- Docker / Docker Compose

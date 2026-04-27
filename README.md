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

## 구현한 것

### 1. Producer/Consumer 기본 흐름

```
REST API 요청
     │
     ▼
SensorController
     │ POST /api/sensor/{deviceId}
     ▼
SensorProducer ──────────────────────► Kafka Broker
  KafkaTemplate.send(topic, key, msg)   sensor-events Topic
                                        ├── partition 0
                                        ├── partition 1
                                        └── partition 2
                                                │
                                                ▼
                                        SensorConsumer
                                        @KafkaListener
                                        processData()
```

### 2. Consumer Group Rebalancing

Consumer 2개를 같은 groupId로 실행하면 Kafka가 partition을 자동으로 분배합니다.

```
Consumer 1개일 때:
  Consumer 1 → partition 0, 1, 2 (전부 담당)

Consumer 2개일 때 (Rebalancing 발생):
  Consumer 1 → partition 0, 1
  Consumer 2 → partition 2

Consumer 2 종료 시 (Rebalancing 발생):
  Consumer 1 → partition 0, 1, 2 (다시 전부 담당)
```

**직접 확인한 로그:**
```
partitions revoked: [sensor-events-0, sensor-events-1, sensor-events-2]
→ Consumer 1: Assignment(partitions=[sensor-events-0, sensor-events-1])
→ Consumer 2: Assignment(partitions=[sensor-events-2])
```

### 3. offset 기반 재처리

앱을 껐다 켜도 이전에 읽은 위치(offset)를 기억해서 이어서 읽습니다.

```
# 앱 재시작 시 로그
Setting offset for partition sensor-events-0 to committed offset=2
→ "partition 0에서 2개 읽었으니 3번부터 시작"

Found no committed offset for partition sensor-events-1
→ "partition 1은 처음부터 시작"
```

### 4. DLQ (Dead Letter Queue) 패턴

처리 실패 메시지를 버리지 않고 별도 Topic에 보관합니다.

```
sensor-events Topic
     │
     ▼
Consumer 처리 시도
     │
     ├── 성공 → 처리 완료
     │
     └── 실패 → sensor-events-dlq Topic으로 전송
                  (나중에 원인 분석 + 재처리 가능)
```

**직접 확인한 결과:**
```
device-001 temp:36.5 → 처리 완료 ✓
device-002 temp:41.0 → 처리 실패 → DLQ 전송 ✓
device-003 temp:37.2 → 처리 완료 ✓
device-004 temp:45.5 → 처리 실패 → DLQ 전송 ✓
```

---

## 핵심 구현 코드

### Producer — Key로 partition 라우팅

```java
public void send(String deviceId, String message) {
    kafkaTemplate.send(TOPIC, deviceId, message)  // deviceId = Key
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

- `deviceId`를 Key로 사용 → 동일 디바이스 메시지가 항상 같은 partition으로 라우팅 → **순서 보장**

### Consumer — partition·offset 추적 + DLQ

```java
@KafkaListener(topics = "sensor-events", groupId = "sensor-group")
public void consume(
        @Payload String message,
        @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset) {

    try {
        processData(key, message);
    } catch (Exception e) {
        // 처리 실패 → DLQ로 전송 (메시지 유실 방지)
        kafkaTemplate.send(DLQ_TOPIC, key, message);
    }
}
```

---

## Redis Pub/Sub vs Kafka 트레이드오프

| 항목 | Redis Pub/Sub | Kafka |
|------|--------------|-------|
| 메시지 내구성 | 없음 (메모리, Consumer 꺼지면 유실) | 있음 (디스크 저장) |
| 재처리 | 불가 | offset 기반 가능 |
| 순서 보장 | 보장 안 됨 | partition 내 보장 |
| 수평 확장 | 제한적 | Consumer Group으로 자연스럽게 |
| 운영 복잡도 | 낮음 | 높음 (브로커 클러스터) |
| 저지연 | 매우 낮음 | 상대적으로 높음 |
| 적합한 상황 | 실시간 알림, 단순 이벤트 전달 | 대용량, 재처리 필요, 이벤트 소싱 |

**MassFlux-Gateway에서 Redis를 선택한 이유:**
폐쇄망 환경의 브로커 클러스터 운영 부담 > 요구사항(실시간 수위 이벤트 처리)에서 Kafka의 이점.
저지연과 구조 단순성이 해당 도메인에 더 적합했고, 데이터 유실 리스크는 DB 선적재 + 배치 재처리로 보완했습니다.

---

## 실행 방법

### 1. Kafka 브로커 실행

```bash
docker-compose up -d
```

### 2. Topic 생성

```bash
# 메인 Topic
docker exec kafka kafka-topics \
  --create --topic sensor-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# DLQ Topic
docker exec kafka kafka-topics \
  --create --topic sensor-events-dlq \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 3. Spring Boot 실행

```bash
./mvnw spring-boot:run
```

### 4. 메시지 전송 테스트

```bash
# 정상 메시지
curl -X POST http://localhost:8080/api/sensor/device-001 \
  -H "Content-Type: application/json" \
  -d '"temp:36.5"'

# 비정상 메시지 (40도 이상 → DLQ로 이동)
curl -X POST http://localhost:8080/api/sensor/device-002 \
  -H "Content-Type: application/json" \
  -d '"temp:41.0"'
```

### 5. Consumer Group Rebalancing 확인

```bash
# 터미널 1: 기본 실행
./mvnw spring-boot:run

# 터미널 2: 두 번째 Consumer 추가
SERVER_PORT=8081 ./mvnw spring-boot:run
# → Rebalancing 발생, partition 자동 분배 확인
```

### 6. DLQ 모니터링

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-events-dlq \
  --from-beginning
```

---

## 기술 스택

- Java 17
- Spring Boot 3.2
- Spring Kafka
- Apache Kafka 7.4.0 (Confluent)
- Docker / Docker Compose

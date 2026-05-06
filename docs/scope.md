# Kafka 실습 정리 노트

`kafka-practice`는 Kafka를 운영 수준으로 완성한 프로젝트라기보다, Producer,
Consumer group, DLQ 흐름을 직접 확인하기 위해 만든 실습 저장소입니다.

기존 프로젝트에서 Redis Pub/Sub를 선택했던 이유가 있었고, 그 선택과 Kafka의
차이를 코드로 직접 확인해보고 싶어서 만들었습니다. 그래서 이 저장소는 “Kafka를
잘 안다”는 선언보다 “어떤 차이를 직접 확인했는지”를 남기는 쪽이 맞다고 생각합니다.

## 현재 코드로 확인한 것

- Producer가 `deviceId`를 Kafka key로 사용해 메시지를 보냅니다.
- Consumer group을 사용해 partition rebalancing을 확인할 수 있습니다.
- 처리 실패한 온도 메시지를 `sensor-events-dlq` 토픽으로 보냅니다.
- DLQ 분기는 단위 테스트로 검증합니다.

## 아직 다루지 않은 것

- exactly-once processing
- schema registry
- retry topic과 backoff 정책
- consumer lag 모니터링
- 브로커 수준의 성능 벤치마크

추후에는 retry topic과 consumer lag 모니터링까지 붙여서, 단순 실습에서 조금 더
운영에 가까운 구조로 확장해보고 싶습니다.

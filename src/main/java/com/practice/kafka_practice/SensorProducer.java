package com.practice.kafka_practice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "sensor-events";

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
}
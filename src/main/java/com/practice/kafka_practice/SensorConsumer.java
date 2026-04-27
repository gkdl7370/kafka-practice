package com.practice.kafka_practice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String DLQ_TOPIC = "sensor-events-dlq";

    @KafkaListener(topics = "sensor-events", groupId = "sensor-group")
    public void consume(
            @Payload String message,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[Consumer] 수신 - deviceId: {}, message: {}, partition: {}, offset: {}",
                key, message, partition, offset);

        try {
            processData(key, message);
        } catch (Exception e) {
            log.error("[Consumer] 처리 실패 - deviceId: {}, 원인: {} → DLQ 전송", key, e.getMessage());
            kafkaTemplate.send(DLQ_TOPIC, key, message);
        }
    }

    private void processData(String deviceId, String message) {
        // 온도가 40도 이상이면 처리 실패로 간주 (DLQ 테스트용)
        if (message.contains("temp:")) {
            double temp = Double.parseDouble(message.split(":")[1]);
            if (temp >= 40.0) {
                throw new IllegalArgumentException("비정상 온도 감지: " + temp);
            }
        }
        log.info("[Consumer] 처리 완료 - deviceId: {}", deviceId);
    }
}
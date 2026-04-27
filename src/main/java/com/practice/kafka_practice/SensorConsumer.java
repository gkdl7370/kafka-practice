package com.practice.kafka_practice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SensorConsumer {

    @KafkaListener(topics = "sensor-events", groupId = "sensor-group")
    public void consume(
            @Payload String message,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[Consumer] 수신 - deviceId: {}, message: {}, partition: {}, offset: {}",
                key, message, partition, offset);

        processData(key, message);
    }

    private void processData(String deviceId, String message) {
        log.info("[Consumer] 처리 완료 - deviceId: {}", deviceId);
    }
}
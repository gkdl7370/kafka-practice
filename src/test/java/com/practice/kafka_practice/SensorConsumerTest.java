package com.practice.kafka_practice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SensorConsumerTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final SensorConsumer consumer = new SensorConsumer(kafkaTemplate);

    @Test
    @DisplayName("비정상 온도 메시지는 DLQ 토픽으로 전송한다")
    void consumeSendsInvalidTemperatureToDlq() {
        consumer.consume("temp:40.0", "device-1", 0, 7L);

        verify(kafkaTemplate).send("sensor-events-dlq", "device-1", "temp:40.0");
    }

    @Test
    @DisplayName("정상 온도 메시지는 DLQ로 보내지 않는다")
    void consumeDoesNotSendNormalTemperatureToDlq() {
        consumer.consume("temp:25.0", "device-1", 0, 7L);

        verify(kafkaTemplate, never()).send("sensor-events-dlq", "device-1", "temp:25.0");
    }

    @Test
    @DisplayName("임계값 이상의 온도는 예외로 처리한다")
    void validateTemperatureRejectsThresholdAndAbove() {
        assertThatThrownBy(() -> consumer.validateTemperature("temp:45.5"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("45.5");
    }
}

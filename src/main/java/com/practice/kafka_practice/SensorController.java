package com.practice.kafka_practice;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/sensor")
@RequiredArgsConstructor
public class SensorController {

    private final SensorProducer producer;

    @PostMapping("/{deviceId}")
    public String send(@PathVariable String deviceId, @RequestBody String message) {
        producer.send(deviceId, message);
        return "전송 완료: " + deviceId;
    }
}
package com.practice.kafka_practice;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sensor")
@RequiredArgsConstructor
public class SensorController {

    private final SensorProducer producer;

    @PostMapping("/{deviceId}")
    public String send(@PathVariable String deviceId, @RequestBody String message) {
        producer.send(deviceId, message);
        return "queued: " + deviceId;
    }
}

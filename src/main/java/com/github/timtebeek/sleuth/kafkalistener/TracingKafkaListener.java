package com.github.timtebeek.sleuth.kafkalistener;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class TracingKafkaListener {

    public static final String TOPIC = "demo-topic";

    private final TracingService service;

    @Getter
    private TraceDiagnostics traceDiagnostics; // In lieu of actual storage or side-effects

    @KafkaListener(topics = TOPIC, groupId = "demo-group")
    public void listen(
            @Headers Map<String, Object> messageHeaders,
            @Payload String message) throws Exception {
        Map<String, String> spanHeaders = service.getSpanHeaders();
        log.info("Message headers: {}", messageHeaders);
        log.info("Span headers: {}", spanHeaders);
        log.info("Payload: {}", message);
        this.traceDiagnostics = new TraceDiagnostics(messageHeaders, spanHeaders);
    }
}

@Value
class TraceDiagnostics {
    Map<String, Object> messageHeaders;
    Map<String, String> spanHeaders;
}

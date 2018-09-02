package com.github.timtebeek.sleuth.kafkalistener;

import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapInjectAdapter;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.*;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class TracingKafkaListener {
    private final Tracer tracer;

    @Getter
    private final Map<String, TraceDiagnostics> messageTraces = new ConcurrentHashMap<>();

    @KafkaListener(topics = Constants.TOPIC1, groupId = "demo-group1")
    public void listen(
            @Headers Map<String, Object> messageHeaders,
            @Payload String message) throws Exception {
        Map<String, String> spanHeaders = getSpanHeaders();
        log.info("Message headers: {}", messageHeaders);
        log.info("Span headers: {}", spanHeaders);
        log.info("Payload: {}", message);
        messageTraces.put(message, new TraceDiagnostics(messageHeaders, spanHeaders, null));
    }

    @KafkaListener(topics = Constants.TOPIC2, groupId = "demo-group2")
    public void listenTopic2(
            @Header("corporate_trace_id") String corporateTraceId,
            @Payload String message) throws Exception {
        Map<String, String> spanHeaders = getSpanHeaders();
        log.info("corporate_trace_id: {}", corporateTraceId);
        log.info("Span headers: {}", spanHeaders);
        log.info("Payload: {}", message);
        messageTraces.put(message, new TraceDiagnostics(null, spanHeaders, corporateTraceId));
    }

    private Map<String, String> getSpanHeaders() {
        Map<String, String> spanHeaders = new HashMap<>();
        tracer.inject(tracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(spanHeaders));
        return spanHeaders;
    }
}

@Value
class TraceDiagnostics {
    Map<String, Object> messageHeaders;
    Map<String, String> spanHeaders;
    String corporateTraceId;
}

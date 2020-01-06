package com.github.timtebeek.sleuth.kafkalistener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import brave.Tracer;
import brave.Tracing;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TracingKafkaListener {
	private final Tracing tracing;
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
	public void listen2(
			@Header("corporate_trace_id") String corporateTraceId,
			@Payload String message) throws Exception {
		Map<String, String> spanHeaders = getSpanHeaders();
		log.info("corporate_trace_id: {}", corporateTraceId);
		log.info("Span headers: {}", spanHeaders);
		log.info("Payload: {}", message);
		messageTraces.put(message, new TraceDiagnostics(null, spanHeaders, corporateTraceId));
	}

	@KafkaListener(topics = Constants.TOPIC3, groupId = "demo-group3")
	public void listen3(
			@Headers Map<String, Object> messageHeaders,
			@Payload String message) throws Exception {
		Map<String, String> spanHeaders = getSpanHeaders();
		log.info("Message headers: {}", messageHeaders);
		log.info("Span headers: {}", spanHeaders);
		log.info("Payload: {}", message);
		messageTraces.put(message, new TraceDiagnostics(messageHeaders, spanHeaders, null));
	}

	private Map<String, String> getSpanHeaders() {
		Map<String, String> spanHeaders = new HashMap<>();
		tracing.propagation()
				.injector((carrier, key, value) -> ((Map<String, String>) carrier).put(key, value))
				.inject(tracer.currentSpan().context(), spanHeaders);
		return spanHeaders;
	}
}

@Value
class TraceDiagnostics {
	Map<String, Object> messageHeaders;
	Map<String, String> spanHeaders;
	String corporateTraceId;
}

package com.github.timtebeek.sleuth.kafkalistener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import brave.Tracer;
import brave.Tracing;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TracingKafkaListener {
	private final Tracing tracing;
	private final Tracer tracer;

	@Getter
	private final Map<String, Map<String, String>> messageSpanHeaders = new ConcurrentHashMap<>();

	@KafkaListener(topics = Constants.TOPIC1, groupId = "demo-group1")
	public void listen(@Payload String message) throws Exception {
		Map<String, String> spanHeaders = getSpanHeaders();
		log.info("Span headers: {}", spanHeaders);
		log.info("Payload: {}", message);
		messageSpanHeaders.put(message, spanHeaders);
	}

	@KafkaListener(topics = Constants.TOPIC2, groupId = "demo-group2")
	public void listen2(@Payload String message) throws Exception {
		Map<String, String> spanHeaders = getSpanHeaders();
		log.info("Span headers: {}", spanHeaders);
		log.info("Payload: {}", message);
		messageSpanHeaders.put(message, spanHeaders);
	}

	@KafkaListener(topics = Constants.TOPIC3, groupId = "demo-group3")
	public void listen3(@Payload String message) throws Exception {
		Map<String, String> spanHeaders = getSpanHeaders();
		log.info("Span headers: {}", spanHeaders);
		log.info("Payload: {}", message);
		messageSpanHeaders.put(message, spanHeaders);
	}

	private Map<String, String> getSpanHeaders() {
		Map<String, String> spanHeaders = new HashMap<>();
		tracing.propagation()
				.injector((carrier, key, value) -> ((Map<String, String>) carrier).put(key, value))
				.inject(tracer.currentSpan().context(), spanHeaders);
		return spanHeaders;
	}
}

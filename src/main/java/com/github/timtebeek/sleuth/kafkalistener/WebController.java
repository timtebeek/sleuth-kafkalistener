package com.github.timtebeek.sleuth.kafkalistener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class WebController {
	private final KafkaTemplate<String, String> template;

	@GetMapping(value = "/trace")
	public void trace(@RequestHeader("corporate_trace_id") String corporateTraceId) {
		log.info("Received web request with: {}", corporateTraceId);
		Message<String> message = MessageBuilder
				.withPayload("baz")
				.setHeader(KafkaHeaders.TOPIC, Constants.TOPIC2)
				.build();
		log.info("Sending: {}", message);
		template.send(message);
	}

}

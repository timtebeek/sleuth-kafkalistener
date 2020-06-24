package com.github.timtebeek.sleuth.kafkalistener;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.RequestEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@EmbeddedKafka(topics = { Constants.TOPIC1, Constants.TOPIC2, Constants.TOPIC3 })
@Slf4j
public class TracingKafkaListenerTest {

	private static final String CORPORATE_TRACE_ID = "corporate_trace_id";
	private static final String B3 = "b3";

	@Autowired
	private KafkaTemplate<String, String> template;
	@Autowired
	private TracingKafkaListener listener;
	@Autowired
	private TestRestTemplate restTemplate;

	@Before
	public void clear() {
		listener.getMessageSpanHeaders().clear();
	}

	@Test
	public void testSingleRecordCorrelation() throws Exception {
		ListenableFuture<SendResult<String, String>> sent = template.send(Constants.TOPIC1, "foo");
		SendResult<String, String> result = sent.get();

		// Extract producer details
		log.info("SendResult.RecordMetadata: {}", result.getRecordMetadata());
		log.info("SendResult.ProducerRecord: {}", result.getProducerRecord());
		Headers headers = result.getProducerRecord().headers();
		String producerB3 = new String(headers.lastHeader(B3).value());
		String producerTraceId = producerB3.substring(0, 16);
		String producerSpanId = producerB3.substring(17, 32);
		log.info("Producer {}, {}", B3, producerB3);

		// Wait until received, stored and retrieved
		await().untilAsserted(() -> assertThat(listener.getMessageSpanHeaders().get("foo")).isNotNull());
		Map<String, String> traceDiagnostics = listener.getMessageSpanHeaders().get("foo");
		String consumerB3 = traceDiagnostics.get(B3);

		// Verify
		assertThat(consumerB3).startsWith(producerTraceId); // Should continue parent trace
		assertThat(consumerB3).doesNotEndWith(producerSpanId); // New span started for consumer
	}

	@Test
	public void testExtraHeaderPropagated() throws Exception {
		String corporateTraceId = UUID.randomUUID().toString();
		Message<String> message = MessageBuilder
				.withPayload("bar")
				.setHeader(KafkaHeaders.TOPIC, Constants.TOPIC2)
				.setHeader(CORPORATE_TRACE_ID, corporateTraceId)
				.build();
		template.send(message);
		await().untilAsserted(() -> assertThat(listener.getMessageSpanHeaders().get("bar")).isNotNull());
		Map<String, String> traceDiagnostics = listener.getMessageSpanHeaders().get("bar");
		assertThat(traceDiagnostics.get(CORPORATE_TRACE_ID)).isEqualTo(corporateTraceId);
	}

	@Test
	public void testWebHeaderPropagated() throws Exception {
		String corporateTraceId = UUID.randomUUID().toString();
		restTemplate.exchange(RequestEntity.get(URI.create("/trace"))
				.header(CORPORATE_TRACE_ID, corporateTraceId)
				.build(), Void.class);
		await()
				.atMost(Duration.ofSeconds(20))
				.untilAsserted(() -> assertThat(listener.getMessageSpanHeaders().get("baz")).isNotNull());
		Map<String, String> traceDiagnostics = listener.getMessageSpanHeaders().get("baz");
		assertThat(traceDiagnostics.get(CORPORATE_TRACE_ID)).isEqualTo(corporateTraceId);
	}

	@Test
	public void testMultiRecordsAllUniqueTraceIds() throws Exception {
		int numberOfMessagesToSend = 100;
		Map<String, String> sentMessageTraceIds = new HashMap<>();
		for (int i = 0; i < numberOfMessagesToSend; i++) {
			String message = "message-" + i;
			ListenableFuture<SendResult<String, String>> sent = template.send(Constants.TOPIC3, message);
			SendResult<String, String> result = sent.get();

			Headers headers = result.getProducerRecord().headers();
			String producerB3 = new String(headers.lastHeader(B3).value());
			String producerTraceId = producerB3.substring(0, 16);
			sentMessageTraceIds.put(message, producerTraceId);
		}

		// Wait until received, stored and retrieved
		Map<String, Map<String, String>> receivedMessageTraces = listener.getMessageSpanHeaders();
		await().untilAsserted(() -> assertThat(receivedMessageTraces).hasSize(numberOfMessagesToSend));

		// Compare all sent trace ids with the received/extracted trace ids
		for (Map.Entry<String, String> produced : sentMessageTraceIds.entrySet()) {
			assertThat(receivedMessageTraces).hasEntrySatisfying(produced.getKey(),
					tracediagnostics -> assertThat(tracediagnostics.get(B3)).startsWith(produced.getValue()));
		}

		// Verify all unique
		long count = receivedMessageTraces.values().stream()
				.map(it -> it.get(B3))
				.distinct().count();
		assertThat(count).isEqualTo(numberOfMessagesToSend);
	}

}

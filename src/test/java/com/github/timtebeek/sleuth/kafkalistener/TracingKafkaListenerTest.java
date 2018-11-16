package com.github.timtebeek.sleuth.kafkalistener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = "spring.sleuth.propagation-keys=corporate_trace_id")
@EmbeddedKafka
@Slf4j
public class TracingKafkaListenerTest {
    private static final String X_B3_TRACE_ID = "X-B3-TraceId";
    private static final String X_B3_SPAN_ID = "X-B3-SpanId";

    @Autowired
    private KafkaTemplate<String, String> template;
    @Autowired
    private TracingKafkaListener listener;

    @Before
    public void clear() {
        listener.getMessageTraces().clear();
    }

    @Test
    public void testSingleRecordCorrelation() throws Exception {
        ListenableFuture<SendResult<String, String>> sent = template.send(Constants.TOPIC1, "foo");
        SendResult<String, String> result = sent.get();

        // Extract producer details
        log.info("SendResult.RecordMetadata: {}", result.getRecordMetadata());
        log.info("SendResult.ProducerRecord: {}", result.getProducerRecord());
        Headers headers = result.getProducerRecord().headers();
        String producerTraceId = new String(headers.lastHeader(X_B3_TRACE_ID).value());
        String producerSpanId = new String(headers.lastHeader(X_B3_SPAN_ID).value());
        log.info("Producer {}, {}", X_B3_TRACE_ID, producerTraceId);
        log.info("Producer {}, {}", X_B3_SPAN_ID, producerSpanId);

        // Wait until received, stored and retrieved
        await().untilAsserted(() -> assertThat(listener.getMessageTraces().get("foo")).isNotNull());
        TraceDiagnostics traceDiagnostics = listener.getMessageTraces().get("foo");
        String consumerTraceId = traceDiagnostics.getSpanHeaders().get(X_B3_TRACE_ID);
        String consumerSpanId = traceDiagnostics.getSpanHeaders().get(X_B3_SPAN_ID);

        // Verify
        assertThat(producerTraceId).isEqualTo(consumerTraceId); // Should continue parent trace
        assertThat(producerSpanId).isNotEqualTo(consumerSpanId); // New span started for consumer
    }

    @Test
    public void testHeadersNotStripped() throws Exception {
        String corporateTraceId = UUID.randomUUID().toString();
        Message<String> message = MessageBuilder
                .withPayload("bar")
                .setHeader(KafkaHeaders.TOPIC, Constants.TOPIC2)
                .setHeader("corporate_trace_id", corporateTraceId)
                .build();
        template.send(message);
        // Listener logs: org.springframework.messaging.MessageHandlingException: Missing header 'corporate_trace_id' for method parameter type [class java.lang.String]
        // But when `spring.sleuth.propagation-keys=corporate_trace_id` is removed, the below works again
        await().untilAsserted(() -> assertThat(listener.getMessageTraces().get("bar")).isNotNull()); // Never reached
        TraceDiagnostics traceDiagnostics = listener.getMessageTraces().get("bar");
        assertThat(traceDiagnostics.getCorporateTraceId()).isEqualTo(corporateTraceId);
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
            String producerTraceId = new String(headers.lastHeader(X_B3_TRACE_ID).value());
            sentMessageTraceIds.put(message, producerTraceId);
        }

        // Wait until received, stored and retrieved
        Map<String, TraceDiagnostics> receivedMessageTraces = listener.getMessageTraces();
        await().untilAsserted(() -> assertThat(receivedMessageTraces).hasSize(numberOfMessagesToSend));

        // Compare all sent trace ids with the received/extracted trace ids
        for (Map.Entry<String, String> produced : sentMessageTraceIds.entrySet()) {
            assertThat(receivedMessageTraces).hasEntrySatisfying(produced.getKey(),
                    tracediagnostics -> assertThat(produced.getValue()).isEqualTo(tracediagnostics.getSpanHeaders().get(X_B3_TRACE_ID)));
        }

        // Verify all unique
        long count = receivedMessageTraces.values().stream().map(TraceDiagnostics::getSpanHeaders).map(it -> it.get(X_B3_TRACE_ID))
                .distinct().count();
        assertThat(count).isEqualTo(numberOfMessagesToSend);
    }
}

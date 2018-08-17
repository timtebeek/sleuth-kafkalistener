package com.github.timtebeek.sleuth.kafkalistener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(topics = TracingKafkaListener.TOPIC)
@Slf4j
public class TracingKafkaListenerTest {
    private static final String X_B3_TRACE_ID = "X-B3-TraceId";
    private static final String X_B3_SPAN_ID = "X-B3-SpanId";

    @Autowired
    private ProducerFactory<String, String> producerFactory;
    @Autowired
    private TracingKafkaListener listener;

    @Test
    public void test() throws Exception {
        // Send through producerFactory
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        ListenableFuture<SendResult<String, String>> sent = template.send(TracingKafkaListener.TOPIC, "foo");
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
        await().untilAsserted(() -> assertThat(listener.getTraceDiagnostics()).isNotNull());
        TraceDiagnostics traceDiagnostics = listener.getTraceDiagnostics();
        String consumerTraceId = traceDiagnostics.getSpanHeaders().get(X_B3_TRACE_ID);
        String consumerSpanId = traceDiagnostics.getSpanHeaders().get(X_B3_SPAN_ID);

        // Verify
        assertThat(producerTraceId).isEqualTo(consumerTraceId); // Should continue parent trace
        assertThat(producerSpanId).isNotEqualTo(consumerSpanId); // New span started for consumer
    }
}

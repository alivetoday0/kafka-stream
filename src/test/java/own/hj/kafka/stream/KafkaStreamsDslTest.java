package own.hj.kafka.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaStreamsDslTest {

    private final static String INPUT_TOPIC_NAME = "input-topic";
    private final static String OUTPUT_TOPIC_NAME = "output-topic";
    private final static Serde STRING_SERDE = Serdes.String();
    private final static Serde LONG_SERDE = Serdes.Long();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setup() {
        StreamsBuilder builder = streamsBuilder();
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE.getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE.getClass().getName());
        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, STRING_SERDE.serializer(), STRING_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, STRING_SERDE.deserializer(), LONG_SERDE.deserializer());
    }

    StreamsBuilder streamsBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(INPUT_TOPIC_NAME, Consumed.with(STRING_SERDE, STRING_SERDE));

        KTable<String, Long> wordCounts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count();

        wordCounts.toStream(Named.as("producer-count")).to(OUTPUT_TOPIC_NAME, Produced.with(STRING_SERDE, LONG_SERDE));

        return builder;
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldFlushStoreForFirstTime() {
        inputTopic.pipeInput("all streams lead to kafka hello kafka streams join kafka summit");
        assertEquals(3L, outputTopic.readKeyValuesToMap().get("kafka").longValue());
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("all streams lead to kafka hello kafka streams join kafka summit");
        assertEquals(6L, outputTopic.readKeyValuesToMap().get("kafka").longValue());
        assertTrue(outputTopic.isEmpty());
    }
}

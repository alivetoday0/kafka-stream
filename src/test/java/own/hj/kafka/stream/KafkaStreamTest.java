package own.hj.kafka.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reference URL: https://docs.confluent.io/platform/current/streams/developer-guide/test-streams.html
 */
@DisplayName("A Kafka Stream")
class KafkaStreamTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private KeyValueStore<String, Long> store;

    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    @BeforeEach
    void setup() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("aggStore"),
                        Serdes.String(),
                        Serdes.Long()).withLoggingDisabled(),
                "aggregator");
        topology.addSink("sinkProcessor", "result-topic", "aggregator");

        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("should flush store for first time")
    void shouldFlushStoreForFirstTime() {
        inputTopic.pipeInput("a", 21L);
        assertEquals(new KeyValue<>("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void shouldNotUpdateStoreForSmallerValue() {
        inputTopic.pipeInput("a", 1L);
        assertEquals(21L, store.get("a").longValue());
        assertEquals(new KeyValue<>("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void shouldNotUpdateStoreForLargerValue() {
        inputTopic.pipeInput("a", 42L);
        assertEquals(42L, store.get("a").longValue());
        assertEquals(new KeyValue<>("a", 42L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void shouldUpdateStoreForNewKey() {
        inputTopic.pipeInput("b", 21L);
        assertEquals(21L, store.get("b").longValue());
        assertEquals(new KeyValue<>("a", 21L), outputTopic.readKeyValue());
        assertEquals(new KeyValue<>("b", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void shouldPunctuateIfEvenTimeAdvances() {
        final Instant recordTime = Instant.now();
        inputTopic.pipeInput("a", 1L, recordTime);
        assertEquals(new KeyValue<>("a", 21L), outputTopic.readKeyValue());

        inputTopic.pipeInput("a", 1L, recordTime);
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
        assertEquals(new KeyValue<>("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void shouldPunctuateIfWallClockTimeAdvances() {
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        assertEquals(new KeyValue<>("a", 21L), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    public static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long> {
        @Override
        public Processor<String, Long> get() {
            return new CustomMaxAggregator();
        }
    }

    public static class CustomMaxAggregator implements Processor<String, Long> {

        ProcessorContext context;
        private KeyValueStore<String, Long> store;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
            context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore());
            store = context.getStateStore("aggStore");
        }

        @Override
        public void process(String key, Long value) {
            final  Long oldValue = store.get(key);
            if(oldValue == null || value > oldValue) {
                store.put(key, value);
            }
        }

        @Override
        public void close() { }

        private void flushStore() {
            final KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                final KeyValue<String, Long> next = it.next();
                context.forward(next.key, next.value);
            }
        }
    }
}

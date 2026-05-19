package company.vk.edu.distrib.compute.vladislav_guzov;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

class AuditServiceImpl implements AuditService {
    private static final String TOPIC = "audit";

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    private KafkaConsumer<String, String> consumer;
    private Thread pollThread;

    AuditServiceImpl(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }

    @Override
    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));

        pollThread = new Thread(this::pollLoop, "audit-consumer");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private void pollLoop() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        events.add(parseEvent(record.value()));
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // normal shutdown
        } finally {
            consumer.close();
        }
    }

    private static AuditEvent parseEvent(String value) {
        String[] parts = value.split("\t", 3);
        return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
    }

    @Override
    public void stop() {
        if (consumer == null) {
            return;
        }
        consumer.wakeup();
        try {
            pollThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(events);
    }
}

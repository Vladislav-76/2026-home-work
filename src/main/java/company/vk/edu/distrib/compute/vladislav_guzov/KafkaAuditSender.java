package company.vk.edu.distrib.compute.vladislav_guzov;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaAuditSender implements AutoCloseable {
    private static final String TOPIC = "audit";

    private final KafkaProducer<String, String> producer;
    private volatile boolean async;

    KafkaAuditSender(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    void setAsync(boolean value) {
        this.async = value;
    }

    void send(AuditEvent event) {
        String value = event.method() + "\t" + event.id() + "\t" + event.timestamp();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.id(), value);
        if (async) {
            producer.send(record);
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}

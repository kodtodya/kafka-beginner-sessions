package com.kodtodya.sessions.kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaBasicProducerDemo{

    private static final String TOPIC = "test";

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {

            for (long i = 0; i < 10; i++) {
                String message = "Message -> " + i;
            	final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, message);
                producer.send(record);
                System.out.println("Message sent to broker :"+message);
                Thread.sleep(1000L);
            }

        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);


    }

}
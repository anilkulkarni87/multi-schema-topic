package com.lina.multischematopic;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class KafkaAvroGenericRecordConsumer {
    private static final Logger logger = LogManager.getLogger("KafkaAvroGenericRecordConsumer");

    private static final String TOPIC = "linamultischematopic1";

    public static void main(String[] args) {
        logger.info("Starting the process");
        //Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"multischemartopicconsumer");

        //Set value for a new property
        properties.setProperty("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());

        Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC));
        IntStream.range(1, 100).forEach(index -> {
            final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            if (records.count() == 0) {
                System.out.println("None found");
            } else {
                records.forEach(record -> {
                    GenericRecord recValue = record.value();
                    System.out.println(record.key());
                    System.out.println(record.headers().headers(""));
                    System.out.printf("topic: %s partition: %d offset: %d Value: %s \n", record.topic(), record.partition(), record.offset(), recValue);
                });
            }
        });
    }




    }


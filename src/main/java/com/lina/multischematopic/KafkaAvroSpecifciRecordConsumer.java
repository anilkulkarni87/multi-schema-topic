package com.lina.multischematopic;

import com.lina.Movie;
import com.lina.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.JsonUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class KafkaAvroSpecifciRecordConsumer<T extends SpecificRecordBase & SpecificRecord> {

    private static final Logger logger = LogManager.getLogger("KafkaAvroSpecifciRecordConsumer");

    private static final String TOPIC = "linamultischematopic3";

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
        properties.setProperty(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY,TopicRecordNameStrategy.class.getName());
        properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        //Set value for a new property
        properties.setProperty("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());

        Consumer<String, SpecificRecord> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC));
        IntStream.range(1, 50).forEach(index -> {
            final ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            if (records.count() == 0) {
                System.out.println("None found");
            } else {
                records.forEach(record -> {
                    System.out.println(record.value().getClass().getName());
                    System.out.println(record.value());
                    record.headers().forEach(header -> System.out.println("Header Key is : "+header.key()+" Value is: "+new String(header.value(), StandardCharsets.UTF_8)));
                    if(record.key().equalsIgnoreCase("movieType")){
                        Movie recValue = (Movie) record.value();
                        System.out.printf("topic: %s partition: %d offset: %d MovieName: %s MovieGenre: %s\n", record.topic(), record.partition(), record.offset(), recValue.getMovieName(), recValue.getGenre());
                    } else if(record.key().equalsIgnoreCase("userType")){
                        User recValue = (User) record.value();
                        System.out.printf("topic: %s partition: %d offset: %d FirstName: %s LastName: %s\n", record.topic(), record.partition(), record.offset(), recValue.getFirstName(), recValue.getLastName());
                    }

                });
            }
        });


    }

    final void consume(Properties properties) throws IOException {

    }
}

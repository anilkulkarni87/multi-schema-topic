package com.lina.multischematopic;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.Properties;

public class KafkaAvroGenericRecordProducer {
    private static final Logger logger = LogManager.getLogger("KafkaAvroGenericRecordProducer");

    private static final String TOPIC = "linamultischematopic1";

    public static void main(String[] args) throws IOException {
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
        logger.info("Starting the process");
        //Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        //Set value for a new property
       properties.setProperty("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());

        //Create Kafka producer and set properties
        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties);
        //Create 2 Kafka Messages
        ProducerRecord<String, GenericRecord>  userAvroRecord = new ProducerRecord<>(TOPIC,"userType", createUserPayload());
        ProducerRecord<String, GenericRecord> movieAvroRecord = new ProducerRecord<>(TOPIC,"movieType", createMovieAvroPayload());
        ProducerRecord<String, GenericRecord> employeeAvroRecord = new ProducerRecord<>(TOPIC,"employeeType", createEmployeePayload());

        producer.send(userAvroRecord);
        producer.send(movieAvroRecord);
        producer.send(employeeAvroRecord);

        producer.flush();
        producer.close();

    }

    private static GenericRecord createMovieAvroPayload() throws IOException {
        //Create schema from avsc file
        Schema movieSchema = new Schema.Parser().parse(new ClassPathResource("avro/movie-v1.avsc").getInputStream());

        //Create avro message with define schema
        GenericRecord avroMessage = new GenericData.Record(movieSchema);
        //Populate Avro message
        avroMessage.put("movie_name","Gandadha Gudi");
        avroMessage.put("genre","Action");
        return avroMessage;
    }

    private static GenericRecord createUserPayload() throws IOException {
        Schema userSchema = new Schema.Parser().parse(new ClassPathResource("avro/user-v1.avsc").getInputStream());
        GenericRecord avroMessage = new GenericData.Record(userSchema);
        avroMessage.put("first_name","Raj");
        avroMessage.put("last_name","Kumar");
        return avroMessage;
    }

    private static GenericRecord createEmployeePayload() throws IOException {
        Schema employeeSchema = new Schema.Parser().parse(new ClassPathResource("avro/employee-v1.avsc").getInputStream());
        GenericRecord avroMessage = new GenericData.Record(employeeSchema);
        avroMessage.put("name","Einstein");
        avroMessage.put("age",35);
        avroMessage.put("title","Manager");
        return avroMessage;
    }
}

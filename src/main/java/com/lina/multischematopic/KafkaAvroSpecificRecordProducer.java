package com.lina.multischematopic;

import com.lina.Employee;
import com.lina.Movie;
import com.lina.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.Properties;

public class KafkaAvroSpecificRecordProducer {

    private static final Logger logger = LogManager.getLogger("KafkaAvroSpecificRecordProducer");

    private static final String TOPIC = "linamultischematopic3";

    public static void main(String[] args) throws IOException {
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
        logger.info("Starting the process");
        //Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("auto.register.schemas","false");
        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        //Set value for a new property
        properties.setProperty("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());

        //Create Kafka producer and set properties
        Headers headers = new RecordHeaders();
        headers.add("Id","Id".getBytes());
        headers.add("AppId","AppId".getBytes());
        headers.add("Type","Type".getBytes());
        headers.add("geoLocation","geoLocation".getBytes());
        headers.add("EventTime","EventTime".getBytes());
        headers.add("SystemTime","SystemTime".getBytes());
        headers.add("Tokenizer","Tokenizer".getBytes());

        Producer<String, SpecificRecord> producer = new KafkaProducer<String, SpecificRecord>(properties);
        ProducerRecord<String, SpecificRecord> movieAvroRecord = new ProducerRecord<>(TOPIC,null,"movieType", createMovieAvroPayload(), headers);
        ProducerRecord<String, SpecificRecord> userAvroRecord = new ProducerRecord<>(TOPIC,"userType", createUserAvroPayload());
        ProducerRecord<String, SpecificRecord> employeeAvroRecord = new ProducerRecord<>(TOPIC,"userType", createEmployeeAvroPayload());

        producer.send(movieAvroRecord);
        producer.send(userAvroRecord);
        producer.send(employeeAvroRecord);

        producer.flush();
        producer.close();
    }

    private static SpecificRecord createMovieAvroPayload() throws IOException {
        Movie movie = Movie.newBuilder()
                .setMovieName("Pailwaan")
                .setGenre("Action")
                .build();
        return movie;
    }

    private static SpecificRecord createInvalidMovieAvroPayload() throws IOException {
        Movie movie = new Movie();
                movie.setMovieName("DummyMovie");
        return movie;
    }

    private static SpecificRecord createUserAvroPayload() throws IOException {
        User user = User.newBuilder()
                .setFirstName("Sachin")
                .setLastName("Tendulkar")
                .build();
        return user;
    }

    private static SpecificRecord createEmployeeAvroPayload() throws IOException {
        Employee employee = Employee.newBuilder()
                .setName("Someguy")
                .setAge(35)
                .setTitle("SomeTitekle")
                .build();
        return employee;
    }
}

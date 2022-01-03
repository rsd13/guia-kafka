package com.github.rsd13.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";

        //propiedades del productor
        Properties properties = new Properties();
        // añadimos las propiedades de https://kafka.apache.org/documentation/#configuration
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //key y value ayuda a saber que tipo de valor esta enviando a Kafka para serilizarlo
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //constructor del productor con key/value a String
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //crear productor record
        ProducerRecord<String,String > record =
                new ProducerRecord<String, String>("first_topic","hola mundo!");

        //enviar datos
        producer.send(record);
        //Espere a que se entreguen todos los mensajes de la cola del productor.
        producer.flush();
        producer.close();

    }
}

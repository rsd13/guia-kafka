package com.github.rsd13.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        String bootstrapServers = "localhost:9092";
        String topic ="first_topic";
        String groupID ="my-tutoria-app-2";

        //propiedades del consumidor
        Properties properties = new Properties();
        // añadimos las propiedades de https://kafka.apache.org/documentation/#configuration
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //key y value ayuda a saber que tipo de valor esta enviando a Kafka para serilizarlo
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        /*
        earliest-> se quiere leer desde el principio
        latest-> se quiere leer los mensajes nuevos
        none-> no se guardan los offset
         */

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //crea el consume
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        System.out.println("xxxxxxx");

        //se subscribe el consumer al topic
        //consumer.subscribe(Collections.singleton(topic));
        //para mas de un topic
        consumer.subscribe(Arrays.asList(topic));
        //poll del data

        while(true){

            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record: records){
               logger.info("Key: " + record.key() + ", Value: " + record.value());
               logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
           }
        }

    }
}

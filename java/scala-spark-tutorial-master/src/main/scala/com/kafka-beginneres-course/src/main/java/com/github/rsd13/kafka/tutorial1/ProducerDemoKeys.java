package com.github.rsd13.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger =LoggerFactory.getLogger((ProducerDemoKeys.class));
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
        for(int i = 0; i<10;i++) {
            String topic = "first_topic";
            String value = "hola mundo" + i;
            String key = "Key_" + i;

            //crear productor record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key: " + key);
            /*
            A poner una key se garantiza que esa key siempre vaya a la misma partición
            id_0-> partition: 1
            id_1-> partition: 2
            id_2-> partition: 2
            id_3-> partition: 0
            id_4-> partition: 2
            id_5-> partition: 2
            id_6-> partition: 1
            id_7-> partition: 1
            id_8-> partition: 0
            id_9-> partition: 0
             */



            //enviar datos
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //la funcion callback se ejecuta cuando termina send y y es exitoso
                    if (e == null) {
                        //si no hay excepcion
                        logger.info("Nuevo metadata");
                        logger.info("Topic: " + recordMetadata.topic());
                        logger.info("Partition: " + recordMetadata.partition());
                        logger.info("Offset: " + recordMetadata.offset());
                        logger.info("Timestamp: " + recordMetadata.timestamp());
                    } else {
                        //mostramos las excepciones
                        logger.error("Error produciendo: ", e);

                    }
                }
            }).get(); //bloquea el envio para hacerlo sincrono (NO HACERLO EN PRODUCCIÓN)
        }
        //Espere a que se entreguen todos los mensajes de la cola del productor.
        producer.flush();
        producer.close();

    }
}

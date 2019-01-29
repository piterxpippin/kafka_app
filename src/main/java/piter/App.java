package piter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("request.timeout.ms", 10000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Runnable producerTask = () -> {
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
            while (true) {
                try {
                    System.out.println("KafkaProducer sending...");
                    kafkaProducer.send(new ProducerRecord<>("my_hello_world_topic", new Date().toString(), "Hello World!"));
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    kafkaProducer.close();
                }
            }
        };
        final Thread producerThread = new Thread(producerTask);

        Runnable consumerTask = () -> {
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
            kafkaConsumer.subscribe(Arrays.asList("my_hello_world_topic"));
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
            while (true) {
                records = kafkaConsumer.poll(1000);
                System.out.println("KafkaConsumer polling...");
                for (ConsumerRecord record : records) {
                    System.out.println("Consumer gets record: [" + record.offset() + ", " + record.key() + ", " + record.value() + "]");
                }
            }
        };
        final Thread consumerThread = new Thread(consumerTask);

        producerThread.start();
        consumerThread.start();

        try {
            producerThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

package io.github.datheng.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        props.put("group.id", groupId);
        // none/earliest/latest
        props.put("auto.offset.reset", "earliest");
        props.put("partition.assigment.strategy", CooperativeStickyAssignor.class.getName());
//        props.put("group.instance.id", "..."); // strategy for static assignment


        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Get the reference to the mai thread
        final Thread mainThread = Thread.currentThread();
        // Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detect a shutdown hook, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();
            // join the main thread to allow the execution of the code in main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data
            while (true) {
                log.info("Polling records from Kafka...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {}, Value: {}", record.key(), record.value());
                    log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                }
            }
        }catch (WakeupException e) {
            log.info("Consumer is starting to shut down");

        }catch (Exception e) {
            log.error("Un expected exception in the consumer.", e);
        }finally {
            consumer.close(); // close the consumer, this will also commit the offsets
            log.info("Consumer is now gracefully shutting down");
        }
    }
}

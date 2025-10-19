package io.github.datheng.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";


        // create Producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String topic = "wikimedia.recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // 2. Build the EventSource
        HttpUrl httpUrl = HttpUrl.get(url); // Replace with your SSE endpoint


        BackgroundEventSource bes = new BackgroundEventSource.Builder(eventHandler,
                new EventSource.Builder(
                        ConnectStrategy.http(httpUrl)
                                .connectTimeout(5, TimeUnit.SECONDS)
                                .header("User-Agent", "")
                        // connectTimeout and other HTTP options are now set through
                        // HttpConnectStrategy
                )
        )
                .threadPriority(Thread.MAX_PRIORITY)
                // threadPriority, and other options related to worker threads,
                // are now properties of BackgroundEventSource
                .build();
        bes.start();



        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}

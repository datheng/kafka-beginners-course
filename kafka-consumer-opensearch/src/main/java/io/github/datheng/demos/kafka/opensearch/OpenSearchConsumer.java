package io.github.datheng.demos.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(){
        String groupId = "wikimedia-consumer";

        // create consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        props.put("group.id", groupId);
        // none/earliest/latest
        props.put("auto.offset.reset", "latest");

        // create a consumer
        return new KafkaConsumer<String, String>(props);
    }

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // first create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create our kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        try(openSearchClient;kafkaConsumer) {
            // we need create the index on OpenSearch if it does not exist
            boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if( ! indexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index has been created.");
            }else {
                log.info("The wikimedia index already exist.");
            }

            // main code logic
            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(3000));
                int n = consumerRecords.count();
                log.info("Consumer {} records. ", n);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                    // send the record into OpenSearch
                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(consumerRecord.value(), XContentType.JSON);

                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        log.info("Inserted 1 document into OpenSearch ID: {}", response.getId());
                    }catch (Exception e){

                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        // close things
    }
}

package com.mmalkiew.example.skt.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String CONSUMER_KEY = "****";
    private static final String CONSUMER_SECRET = "****";
    private static final String TOKEN = "****";
    private static final String TOKEN_SECRET = "****";

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        /** set up blocking queue **/
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(1000);
        //  create twitter client
        Client client = createTwitterClient(messageQueue);
        client.connect();

        //  create a kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        //  add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("stopping application...");
            LOGGER.info("shutting down client from twitter...");
            client.stop();
            LOGGER.info("closing producer...");
            kafkaProducer.close();
            LOGGER.info("Done!");
        }));

        //  loop to send tweets to kafka
        while(!client.isDone()) {
            String message = null;
            try {
                message = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Twitter poll error: {}", e);
                client.stop();
            }

            if (message != null) {
                LOGGER.info("polled message: {}", message);
                kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, message), new Callback() {

                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            LOGGER.error("Error occurred: {}", e.getMessage());
                        }
                    }
                });
            }
        }

        LOGGER.info("Application end...");
    }

    private Client createTwitterClient(BlockingQueue<String> messageQueue) {


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin", "sport");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth =  new OAuth1(CONSUMER_KEY,
                                                  CONSUMER_SECRET,
                                                  TOKEN,
                                                  TOKEN_SECRET);

        return new ClientBuilder().name("hosebird-client-01")
                                  .hosts(hosebirdHosts)
                                  .authentication(hosebirdAuth)
                                  .endpoint(hosebirdEndpoint)
                                  .processor(new StringDelimitedProcessor(messageQueue)).build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //  create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //  high throughput producer (at expense of bit latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<String, String>(properties);

    }
}

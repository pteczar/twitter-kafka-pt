package com.github.pteczar.kafka.twitter_kafka;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Properties;


public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String ConsumerKey ="ZZH1uu1KsFhIWClNOPIGZUtzg";
    String ConsumerSecret = "NZZjWPAvNvTwaMqxFFKrABEK76yqnX7JxkZT4RFYmEQfzz4JhJ";
    String TokenKey = "924513445332094977-M0yAPpAoox3endVL98T0cimkMtNgu5V";
    String TokenSecret = "A78jwrMCRkUgoDOB6YBQGnlT1E94QmNI76hiiCs9tFs0L";

    public TwitterProducer() {}

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //creating a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //creating a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          logger.info("stopping application...");
          logger.info("shutting down twitter client...");
          client.stop();
          logger.info("shutting down producer");
          producer.close();
          logger.info("finito!");
        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets",null, msg),new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }

        }
        logger.info("End of application");
    }



    public Client createTwitterClient(BlockingQueue<String> msgQueue) {





        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("coronavirus");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(ConsumerKey, ConsumerSecret,TokenKey,TokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

        public KafkaProducer<String, String> createKafkaProducer() {

        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

     // making producer safe

     properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
     properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
     properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
     properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // our Kafka is 2.00 5 is a good choice


    //create the producer

        KafkaProducer <String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
        }

}

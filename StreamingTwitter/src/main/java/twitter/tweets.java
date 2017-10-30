package twitter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.Status;
import twitter4j.Twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class tweets {

    final static String consumerKey = "n5xFlVX5wa7uP3MEsg5arezV5";
    final static String consumerSecret = "hdwe4ibr3ocC5dKTbL7CrHMfYSE2yqFuCBUtptsnVtj01M0zJ8";
    final static String accessToken = "338489240-vyFcgqw0aRppL2FlvFThUUQ8dk16BpHreH3nQqcf";
    final static String accessTokenSecret = "AasuHh3o38pGBf3R67rpIfMuTMuUB65XtEsgNPw6sY3H4";

    static  String  topic="tweets";
    static ProducerRecord<String,String > record;
    static Producer producer;
   static Gson gson = new Gson();
   static  BufferedWriter bWriter;
   static String mes;
   static List<String> hh;
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf=new SparkConf().setMaster("local[2]").setAppName("twitterExample");
        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(sparkConf,new Duration(10000));
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);


        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producer=new KafkaProducer<String,String>(properties);
        JavaReceiverInputDStream<Status> veriler= TwitterUtils.createStream(javaStreamingContext);


      //  System.out.println("cıkan sonuç bu::"+mes);
        JavaDStream<String > maplenmisVeri=veriler.map(new Function<Status, String>() {
            public String call(Status status) throws Exception {
                String mesaj=status.getText();
                 mes=gson.toJson(mesaj);
                System.out.println( "sonuç:"+mes);
                File file = new File("D:\\big data egitimi\\dosya.json");
                if (!file.exists()) {
                    file.createNewFile();
                }

                FileWriter fileWriter = new FileWriter(file, true);
                bWriter = new BufferedWriter(fileWriter);
                bWriter.write(mes.toString()+"<>");
                record=new ProducerRecord<String, String>(topic,mes);
                producer.send(record);
                return mes;

            }
        });

        maplenmisVeri.print();
        javaStreamingContext.start();



        bWriter.close();
        producer.close();
    }
}

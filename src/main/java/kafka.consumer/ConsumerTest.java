//package kafka.consumer;
//
///**
// * Created by tianzhongqiu on 2018/5/7.
// *
// * A test comsumer for kafkaData
// *
// */
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import java.util.*;
//
//public class ConsumerTest {
//
//    public static Consumer<String, String> kafkaConsumer;
//    private static String BOOTSTRAP_SERVERS = "NM-402-SA5212M4-BIGDATA-2016-378.BIGDATA.CHINATELECOM.CN:9091";
//
//    public static synchronized Consumer<String, String> createConsumer(String groupId) {
//        if (kafkaConsumer != null) {
//            return kafkaConsumer;
//        }
//
//        System.out.println("创建consumer配置");
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS );
//        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
//        props.put("enable.auto.commit", "true");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        System.out.println("创建consumer配置");
//        kafkaConsumer= new KafkaConsumer<String,String>(props);
//        kafkaConsumer.subscribe(Arrays.asList("logauditHDFS"));
//        return kafkaConsumer;
//    }
//    public static void consumer() {
//        kafkaConsumer=createConsumer("656592");
//        System.out.println("开始消费数据");
//        while (true) {
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//            for (ConsumerRecord<String, String> record : records)
//            {
//                String msg = record.value();
//                System.out.println(msg);
//            }
//        }
//    }
//
//
//    public static void main(String[] args) {
//        consumer();
//        }
//
//    }
//

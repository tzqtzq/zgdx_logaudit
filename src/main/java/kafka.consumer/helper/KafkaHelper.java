package kafka.consumer.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;


/**
 * Created by tianzhongqiu on 2018/6/13.
 */

public class KafkaHelper {
//    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(KafkaHelper.class);

    /**
     * 获取kafka logSize
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public static long getLogSize(String host,int port,String topic,int partition){
        String clientName = "Client_" + topic + "_" + partition;
        Broker leaderBroker = getLeaderBroker(host, port, topic, partition);
        String reaHost = null;
        if (leaderBroker != null) {
            reaHost = leaderBroker.host();
        }else {
            System.out.println("Partition of Host is not find");
        }
        SimpleConsumer simpleConsumer = new SimpleConsumer(reaHost, port, 10000, 64*1024, clientName);
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("Error fetching data Offset , Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);

//        log.info( topic+" Partiton"+partition+"logSize "+offsets);

        return offsets[0];
    }

    /**
     * 获取broker ID
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public static Integer getBrokerId(String host,int port,String topic,int partition){
        Broker leaderBroker = getLeaderBroker(host, port, topic, partition);
        if (leaderBroker != null) {
//            log.info( topic+" Partiton"+partition+"leaderBrokerid "+leaderBroker.id());
            return leaderBroker.id();
        }
//        log.info( topic+" Partiton"+partition+"leaderBrokerid "+" null");
        return null;
    }
    /**
     * 获取leaderBroker
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private static Broker getLeaderBroker(String host,int port,String topic,int partition){
        String clientName = "Client_Leader_LookUp";
        SimpleConsumer consumer = null;
        PartitionMetadata partitionMetaData = null;
        try {
            consumer = new SimpleConsumer(host, port, 10000, 64*1024, clientName);
            List<String> topics = new ArrayList<String>();
            topics.add(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse reponse = consumer.send(request);
            List<TopicMetadata> topicMetadataList = reponse.topicsMetadata();
            for(TopicMetadata topicMetadata : topicMetadataList){
                for(PartitionMetadata metadata : topicMetadata.partitionsMetadata()){
                    if (metadata.partitionId() == partition) {
                        partitionMetaData = metadata;
                        break;
                    }
                }
            }
            if (partitionMetaData != null) {
//                log.info( topic+" Partiton"+partition+"LeaderBroker "+partitionMetaData.leader());
                return partitionMetaData.leader();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}

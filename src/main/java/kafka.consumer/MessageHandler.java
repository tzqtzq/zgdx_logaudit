//package kafka.consumer;
//
///**
// * Created by tianzhongqiu on 2018/5/7.
// */
//
//import com.alibaba.fastjson.JSON;
//import com.coocaa.salad.core.entity.AdScheduleEntity;
//import com.coocaa.salad.stat.entity.AdOrderEntity;
//import com.coocaa.salad.stat.entity.CustomerEntity;
//import com.coocaa.salad.stat.logFile.MyLogger;
//import com.coocaa.salad.stat.model.ClientRequestsModel;
//import com.coocaa.salad.stat.model.StatisticsModel;
//import com.coocaa.salad.stat.service.ConstantService;
//import com.origin.eurybia.utils.JsonUtils;
//import com.origin.eurybia.utils.StringUtils;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.message.MessageAndMetadata;
//import org.apache.log4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class MessageHandler extends Thread {
//
//    private static final Logger logger = Logger.getLogger("messageHandler");
//    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MessageHandler.class);
//    KafkaStream<byte[], byte[]> kafkaStreams;
//
//    public MessageHandler(KafkaStream<byte[], byte[]> kafkaStreams) {
//        super();
//        this.kafkaStreams = kafkaStreams;
//    }
//
//    public void run() {
//        //处理kafka数据
//        ConsumerIterator<byte[], byte[]> streamIterator = kafkaStreams.iterator();
//        ClientRequestsModel model = new ClientRequestsModel();
//        while (streamIterator.hasNext()) {
//            log.info("开始处理kafka数据。。。");
//            MessageAndMetadata<byte[], byte[]> record = streamIterator.next();
//            String message = new String(record.message());
//            log.info("{} topic 的数据，写入{}日志文件中。。。", record.topic(), record.topic());
//            if (record.topic().equals("adPv")) {
//                model = (ClientRequestsModel) JsonUtils.json2Object(message, ClientRequestsModel.class);
//                //保存数据到日志文件中
//            } else if (record.topic().equals("adDevReg")) {
//                MyLogger.myInfo2(logger, message);
//            }
//        }
//}

package kafka.consumer.helper;

/**
 * Created by tianzhongqiu on 2018/6/13.
 */
public class KafkaHelperTest {


    /**
     * 获取leaderBroker
     */
    public static void main(String[] args) {

        Long offsets=KafkaHelper.getLogSize(args[0], 9091, args[1], Integer.parseInt(args[2]));
        System.out.println(offsets);

        Integer id=  KafkaHelper.getBrokerId(args[0], 9091, args[1], Integer.parseInt(args[2]));
        System.out.println(id);
    }

}

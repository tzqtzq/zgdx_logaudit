/**
 * Created by tianzhongqiu on 2018/5/30.
 */
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import reids.Jedisutil;
import reids.RedisPoolUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
public class Test4 {
    public static void main(String[] ameinixrgs) {

        String l="18/05/31 10:48:07|~|WARN|~|resourcemanager.RMAuditLogger|~|USER=hjdq\tIP=10.142.121.123\tOPERATION=AM Released Container\tTARGET=Scheduler      RESULT=FAILURE\tDESCRIPTION=Trying to release container not owned by app or with invalid id.\tPERMISSIONS=Unauthorized access or invalid container\tAPPID=application_1527220447442_393275CONTAINERID=container_1527220447442_393275_01_000086";


        String[] split = l.split("\\s{1,}", -1);


        System.out.println( l.split(" ")[3].length());
        System.out.println( l.split(" ")[3]);

    }
}
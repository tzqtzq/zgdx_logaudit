package toolpool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanResult;
import reids.RedisPoolUtil;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by tianzhongqiu on 2018/5/30.
 */
public class Test5 {
    public static void main(String[] args) {

        final JedisPool pool = RedisPoolUtil.initialPool();
        Jedis resource = pool.getResource();

//        Set<String> keys = resource.keys("*");
        ScanResult<String> scan = resource.scan(0);
        for(String key :scan.getResult()){
            System.out.println(key);
        }


    }

}

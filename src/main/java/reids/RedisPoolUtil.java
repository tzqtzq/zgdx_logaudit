package reids;

import java.util.Properties;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by tianzhongqiu on 2018/5/30.
 */
public class RedisPoolUtil {

    private static JedisPool jedisPool = null;
    private static String redisConfigFile = "redis.properties";
    //把redis连接对象放到本地线程中
    private static ThreadLocal<Jedis> local=new ThreadLocal<Jedis>();

    //不允许通过new创建该类的实例
    private RedisPoolUtil() {
    }

    /**
     * 初始化Redis连接池
     */
    public static JedisPool initialPool()  {
        try {
            Properties props = new Properties();
            //加载连接池配置文件
            props.load(RedisPoolUtil.class.getClassLoader().getResourceAsStream(redisConfigFile));
            // 创建jedis池配置实例
            JedisPoolConfig config = new JedisPoolConfig();
            // 设置池配置项值
            config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxActive")));
            config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
            config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
            config.setTestOnBorrow(Boolean.valueOf(props.getProperty("jedis.pool.testOnBorrow")));
            config.setTestOnReturn(Boolean.valueOf(props.getProperty("jedis.pool.testOnReturn")));
            // 根据配置实例化jedis池
            jedisPool = new JedisPool(config, props.getProperty("redis.ip"),
                    Integer.valueOf(props.getProperty("redis.port")),
                    Integer.valueOf(props.getProperty("redis.timeout")),
                    props.getProperty("redis.passWord"));
            System.out.println("线程池被成功初始化");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jedisPool;
    }

    /**
     * 获得连接
     * @return Jedis
     */
    public static Jedis getConn() {
        //Redis对象
        Jedis jedis =local.get();
        if(jedis==null){
            if (jedisPool == null) {
                initialPool();
            }
            jedis = jedisPool.getResource();
            local.set(jedis);
        }
        return jedis;
    }

    //新版本用close归还连接
    public static void closeConn(){
        //从本地线程中获取
        Jedis jedis =local.get();
        if(jedis!=null){
            jedis.close();
        }
        local.set(null);
    }

    //关闭池
    public static void closePool(){
        if(jedisPool!=null){
            jedisPool.close();
        }
    }

    /**
     * 设置key有效时间
     * @param key
     * @param seconds
     */
    public static void expire(String key, int seconds){
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = initialPool();
            jedis = pool.getResource();

            jedis.expire(key, seconds);
        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }
    }

    /**
     * 返还到连接池
     * @param pool
     * @param redis
     */
    public static void returnResource(JedisPool pool, Jedis redis) {
        if (redis != null) {
            pool.returnResource(redis);
        }
    }


}

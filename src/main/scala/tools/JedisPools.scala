package tools

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPools {


    // val jedisPoolConfig = new JedisPoolConfig()

    lazy val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "192.168.181.200", 6379, 3000, "123456", 10)

    def getJedis() = {
        jedisPool.getResource
    }


}

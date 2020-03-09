package com.tangokk.tdqueue.core.redis;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
public class RedisConnection {

    RedisConfiguration conf;

    JedisPool jedisPool;


    public RedisConnection(RedisConfiguration conf) {
        this.conf = conf;
    }

    public synchronized void init() {
        if(jedisPool != null && ! jedisPool.isClosed()) {
            return;
        }
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(jedisPoolConfig, conf.getHost(), conf.getPort(), 10000, conf.getPassword());
        log.info("Redis connect success");
    }


    public Jedis getJedis() {
        if(jedisPool == null || jedisPool.isClosed()) {
            init();
        }
        Jedis jedis = jedisPool.getResource();
        jedis.select(conf.getDatabase());
        return jedis;
    }




}

package com.tangokk.tdqueue.core.repository;


import com.tangokk.tdqueue.core.conf.ClusterConfigurationImpl;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

@Slf4j
public class ReadyQueue {


    private static final String KEY_READY_QUEUE = "key_ready_queue";

    RedisConnection redisConnection;

    public ReadyQueue(RedisConnection redisConnection) {
        this.redisConnection = redisConnection;
    }

    public void pushReadyJobKeys(String [] jobKeys) {
        Jedis jedis = redisConnection.getJedis();
        Transaction tx = jedis.multi();
        for(String jobKey : jobKeys) {
            tx.sadd(getKeyOfReadyQueue(Job.getTopicOfJobKey(jobKey)), jobKey);
        }
        tx.exec();
        jedis.close();
    }


    public void pushReadyJobKey(String jobKey) {
        Jedis jedis = redisConnection.getJedis();
        jedis.sadd(getKeyOfReadyQueue(Job.getTopicOfJobKey(jobKey)), jobKey);
        jedis.close();
    }

    public Collection<String> popReadyJobKeys(String topic, int count) {
        Jedis jedis = redisConnection.getJedis();
        Collection<String> ret = jedis.spop(getKeyOfReadyQueue(topic), count);
        jedis.close();
        return ret;
    }

    private String getKeyOfReadyQueue(String topic) {
        return ClusterConfigurationImpl.getInstance().getClusterName() + "_" + KEY_READY_QUEUE + "_" + topic;
    }


}

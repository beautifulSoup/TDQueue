package com.tangokk.tdqueue.core.repository;


import com.tangokk.tdqueue.core.conf.ClusterConfigurationImpl;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

@Slf4j
public class ReadyQueue {


    private static final String KEY_READY_QUEUE = "key_ready_queue";

    RedisConnection redisConnection;

    public ReadyQueue(RedisConnection redisConnection) {
        this.redisConnection = redisConnection;
    }

    public void pushReadyJobKeys(Collection<String> jobKeys) {
        Jedis jedis = redisConnection.getJedis();
        for(String jobKey : jobKeys) {
            jedis.sadd(getKeyOfReadyQueue(Job.getTopicOfJobKey(jobKey)), jobKey);
        }
    }


    public void pushReadyJobKeys(String jobKey) {
        Jedis jedis = redisConnection.getJedis();
        jedis.sadd(getKeyOfReadyQueue(Job.getTopicOfJobKey(jobKey)), jobKey);
    }

    public Collection<String> popReadyJobKeys(String topic, int count) {
        Jedis jedis = redisConnection.getJedis();
        return jedis.spop(getKeyOfReadyQueue(topic), count);
    }

    private String getKeyOfReadyQueue(String topic) {
        return ClusterConfigurationImpl.getInstance().getClusterName() + "_" + KEY_READY_QUEUE + "_" + topic;
    }


}

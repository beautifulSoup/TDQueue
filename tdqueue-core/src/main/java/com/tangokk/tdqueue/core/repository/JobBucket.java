package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.conf.ClusterConfigurationImpl;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;


@Slf4j
public class JobBucket {

    private static final String KEY_JOB_BUCKET = "tdqueue_job_bucket";

    RedisConnection redisConnection;

    int bucketIndex;

    public JobBucket(RedisConnection redisConnection, Integer bucketIndex) {
        this.redisConnection = redisConnection;
        this.bucketIndex = bucketIndex;
    }


    Collection<String> popTimeUpJobKeys() {
        Jedis jedis = redisConnection.getJedis();
        Collection<String> timeupJobKeys = jedis.zrangeByScore(getKeyOfJobBucket(), 0, System.currentTimeMillis());
        if(timeupJobKeys !=null && timeupJobKeys.size() > 0) {
            jedis.zrem(getKeyOfJobBucket(), timeupJobKeys.toArray(new String[0]));
        }

        return timeupJobKeys;
    }


    void pushJob(Job job) {
        Jedis jedis = redisConnection.getJedis();
        jedis.zadd(getKeyOfJobBucket(), job.getReadyTime(), job.getKeyOfJob());
    }




    private String getKeyOfJobBucket() {
        return ClusterConfigurationImpl.getInstance().getClusterName() + ":" + KEY_JOB_BUCKET + ":" + bucketIndex;
    }



}

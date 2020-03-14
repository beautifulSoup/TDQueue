package com.tangokk.tdqueue.core.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tangokk.tdqueue.core.conf.ClusterConfigurationImpl;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.json.JsonUtil;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

@Slf4j
public class JobPool {

    private static final String KEY_JOB_POOL = "tangokk_tdqueue_job_pool";


    RedisConnection redisConnection;


    public JobPool(RedisConnection redisConnection) {
        this.redisConnection = redisConnection;
    }

    public void addJob(Job job) {
        Jedis jedis = redisConnection.getJedis();
        String jsonStr = null;
        try {
            jsonStr = JsonUtil.generateJson(job);
        } catch (JsonProcessingException e) {
            log.error("Serialize Job fail", e);
            return;
        }
        jedis.hset(getKeyOfJobPool(), job.getKeyOfJob(), jsonStr);
        jedis.close();
    }


    public Job getJob(String jobKey) {
        Jedis jedis = redisConnection.getJedis();
        String json = jedis.hget(getKeyOfJobPool(), jobKey);
        jedis.close();
        if(StringUtils.isEmpty(json)) {
            return null;
        }
        jedis.close();
        Job ret = null;
        try {
             ret = JsonUtil.parseJson(json, Job.class);
        } catch (JsonProcessingException e) {
            log.error("Deserialize Job fail", e);
        }
        return ret;
    }


    public void removeJobs(Job ... job) {
        Jedis jedis = redisConnection.getJedis();
        String [] jobKeysToDelete = Arrays.stream(job)
            .map(Job::getKeyOfJob)
            .collect(Collectors.toList())
            .toArray(new String[job.length]);
        jedis.hdel(getKeyOfJobPool(), jobKeysToDelete);
        jedis.close();
    }

    public void removeJobs(String ... jobKeys) {
        Jedis jedis = redisConnection.getJedis();
        jedis.hdel(getKeyOfJobPool(), jobKeys);
        jedis.close();
    }




    private String getKeyOfJobPool() {
        return ClusterConfigurationImpl.getInstance().getClusterName() + "_" + KEY_JOB_POOL;
    }



}

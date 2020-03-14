package com.tangokk.tdqueue.core.repository;


import com.sun.istack.internal.Nullable;
import com.tangokk.tdqueue.core.conf.ClusterConfigurationImpl;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

@Slf4j
//TODO when to delete state checklist
public class JobStateChecklist {

    private static final String KEY_JOB_STATE_CHECKLIST = "job_state_checklist";

    RedisConnection redisConnection;

    public JobStateChecklist(RedisConnection redisConnection) {
        this.redisConnection = redisConnection;
    }

    public void setJobState(String jobKey, Integer state) {
        Jedis jedis = redisConnection.getJedis();
        jedis.set(getRedisKeyOfJob(jobKey), state.toString());
        jedis.close();
    }


    @Nullable
    public Integer getJobState(String jobKey) {
        Jedis jedis = redisConnection.getJedis();
        String stateStr =  jedis.get(getRedisKeyOfJob(jobKey));
        jedis.close();
        if(!StringUtils.isEmpty(stateStr)) {
            return Integer.parseInt(stateStr);
        } else {
            return null;  //UNKNOW
        }
    }

    public boolean compareAndSet(String jobKey, Integer newState, Integer ... oldState) {
        Jedis jedis = redisConnection.getJedis();
        String redisKeyOfJob = getRedisKeyOfJob(jobKey);
        jedis.watch(getRedisKeyOfJob(jobKey));
        String data = jedis.get(redisKeyOfJob);
        if(data != null && Arrays.asList(oldState).contains(Integer.parseInt(data))) {
            Transaction tx = jedis.multi();
            tx.set(redisKeyOfJob, newState.toString());
            tx.exec();
            jedis.close();
            return true;
        } else {
            jedis.close();
            return false;
        }

    }

    public void removeJobState(String ... jobKeys) {
        Jedis jedis = redisConnection.getJedis();
        String [] modified = new String[jobKeys.length];
        for(int i=0;i<jobKeys.length;i++) {
            modified[i] = getRedisKeyOfJob(jobKeys[i]);
        }
        jedis.del(modified);
        jedis.close();
    }

    private String getKeyOfStateCheckList() {
        return ClusterConfigurationImpl.getInstance().getClusterName() + "_" + KEY_JOB_STATE_CHECKLIST;
    }


    private String getRedisKeyOfJob(String jobKey) {
        return getKeyOfStateCheckList() + "_" + jobKey;
    }

}

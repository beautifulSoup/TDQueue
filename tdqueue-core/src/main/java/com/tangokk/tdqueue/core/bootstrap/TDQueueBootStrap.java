package com.tangokk.tdqueue.core.bootstrap;

import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobPool;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;
import com.tangokk.tdqueue.core.service.JobManager;
import com.tangokk.tdqueue.core.timer.BucketScanTimer;

public class TDQueueBootStrap {

    public static JobManager bootstrap(RedisConfiguration redisConf, Integer bucketCount) {
        RedisConnection redisConnection = new RedisConnection(redisConf);
        JobPool jobPool = new JobPool(redisConnection);
        DelayQueue delayQueue = new DelayQueue(bucketCount, redisConnection);
        ReadyQueue readyQueue = new ReadyQueue(redisConnection);
        JobStateChecklist checklist = new JobStateChecklist(redisConnection);
        JobManager jobManager = new JobManager(jobPool, delayQueue, readyQueue, checklist);
        BucketScanTimer timer = BucketScanTimer.getInstance();
        timer.startScan(delayQueue, readyQueue, checklist);
        return jobManager;
    }



    public static void main(String [] args) {
        String host = "r-bp11cf8caa7228d4.redis.rds.aliyuncs.com";
        Integer port = 6379;
        String password = "AuthGate1";
        Integer database = 51;

        RedisConfiguration configuration = new RedisConfiguration(host, port, password, database);
        JobManager jobManager = TDQueueBootStrap.bootstrap(configuration, 5);
    }

}

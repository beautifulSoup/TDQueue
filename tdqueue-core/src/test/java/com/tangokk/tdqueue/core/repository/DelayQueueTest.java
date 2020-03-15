package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.util.TestJobUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
@Slf4j
public class DelayQueueTest extends BaseRedisRepoTest {


    DelayQueue delayQueue;

    @Before
    public void init() {
        RedisConnection connection = getConnection();
        connection.getJedis();
        delayQueue = new DelayQueue(5, getConnection());

    }


    @Test
    public void testPushAndPopJobs() throws InterruptedException {
        List<Job> jobs = new ArrayList<>();
        System.out.println(""+System.currentTimeMillis());
        for(int i=0;i<100;i++) {
            String id = "" + System.currentTimeMillis() + i;
            Job job;
            if(i<50) {
                job = TestJobUtil.createJob(id, 1000);
                job.initReadyTime();
            } else {
                job = TestJobUtil.createJob(id, 2000);
                job.initReadyTime();
            }
            jobs.add(job);
        }
        HashMap<String, Job> keyJobMap = new HashMap<>();
        System.out.println(""+System.currentTimeMillis());
        jobs.forEach(j -> {
            keyJobMap.put(j.getKeyOfJob(), j);
        });
        delayQueue.pushJobs(jobs.toArray(new Job[0]));
        System.out.println(""+System.currentTimeMillis());
        List<String> batch1 = delayQueue.popTimeUpJobKeys();
        for(String key : batch1) {
            log.info( key +": " + keyJobMap.get(key).getReadyTime());
        }
        Assert.assertEquals(0,batch1.size());
        Thread.sleep(1002);
        List<String> batch2 = delayQueue.popTimeUpJobKeys();
        Assert.assertEquals(50, batch2.size());
        Thread.sleep(2000);
        List<String> batch3 = delayQueue.popTimeUpJobKeys();
        Assert.assertEquals(50, batch3.size());



    }



}

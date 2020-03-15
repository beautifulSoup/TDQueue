package com.tangokk.tdqueue.core.service;

import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.repository.*;
import com.tangokk.tdqueue.core.timer.BucketScanTimer;
import com.tangokk.tdqueue.core.util.TestJobUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;


@Slf4j
public class JobManagerTest extends BaseRedisRepoTest {


    JobManager jobManager;

    BucketScanTimer timer;


    @Before
    public void init() {
        RedisConnection connection = getConnection();
        JobPool pool = new JobPool(connection);
        JobStateChecklist stateList = new JobStateChecklist(connection);
        ReadyQueue readyQueue = new ReadyQueue(connection);
        DelayQueue delayQueue = new DelayQueue(5, connection);
        jobManager = new JobManager(pool, delayQueue, readyQueue, stateList);
        timer = BucketScanTimer.getInstance();
        timer.startScan(delayQueue, readyQueue, stateList);
    }

    @Test
    public void pushAndPopJob() throws InterruptedException {
        String id = "" + System.currentTimeMillis();
        Job job = TestJobUtil.createJob(id, 2000);
        log.info("time: " +System.currentTimeMillis());
        jobManager.pushJob(job);
        Collection<Job> jobs = jobManager.popJobs(job.getTopic(), 10);
        Assert.assertEquals(0, jobs.size());
        Thread.sleep(3000);
        log.info("time: " +System.currentTimeMillis());
        Collection<Job> jobs2 = jobManager.popJobs(job.getTopic(), 10);
        Assert.assertEquals(1, jobs.size());
    }

    @Test
    public void popJobs() {
    }

    @Test
    public void ackJobProcessed() {
    }

    @Test
    public void removeJob() {
    }
}

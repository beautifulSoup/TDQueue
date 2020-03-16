package com.tangokk.tdqueue.core.service;

import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.repository.*;
import com.tangokk.tdqueue.core.timer.BucketScanTimer;
import com.tangokk.tdqueue.core.util.TestJobUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


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
        Job job = TestJobUtil.createJob(id, 1000);
        log.info("time: " +System.currentTimeMillis());
        jobManager.pushJob(job);
        Collection<Job> jobs = jobManager.popJobs(job.getTopic(), 10);
        Integer state1 = jobManager.jobStateChecklist.getJobState(job.getKeyOfJob());
        Assert.assertEquals(JobState.WAITING.index, (int)state1);
        Assert.assertEquals(0, jobs.size());
        Thread.sleep(3000);
        Integer state2 = jobManager.jobStateChecklist.getJobState(job.getKeyOfJob());
        Assert.assertEquals(JobState.READY.index, (int)state2);
        log.info("time: " +System.currentTimeMillis());
        Collection<Job> jobs2 = jobManager.popJobs(job.getTopic(), 10);
        Assert.assertEquals(1, jobs2.size());
        Integer state3 = jobManager.jobStateChecklist.getJobState(job.getKeyOfJob());
        Assert.assertEquals(JobState.PROCESSING.index, (int) state3);
    }


    @Test
    public void testPushAndPopJobs() throws InterruptedException {
        List<Job> jobs = new ArrayList<>();
        for(int i=0;i<100;i++) {
            String id = "" +System.currentTimeMillis() +i;
            Long delay = 0l;
            if(i < 30) {
                delay = 0l;
            } else if(i < 60) {
                delay = 2000l;
            } else {
                delay = 6000l;
            }
            jobs.add(TestJobUtil.createJob(id, delay));
        }

        jobManager.pushJobs(jobs.toArray(new Job[0]));
        System.out.println("" +System.currentTimeMillis());
        Collection<Job> jobs1 = jobManager.popJobs(TestJobUtil.DEFAULT_TOPIC, 100);
        System.out.println("" +System.currentTimeMillis());
        Assert.assertEquals(30, jobs1.size());
        Thread.sleep(2000);
        System.out.println("" +System.currentTimeMillis());
        Collection<Job> jobs2 = jobManager.popJobs(TestJobUtil.DEFAULT_TOPIC, 100);
        Assert.assertEquals(30, jobs2.size());
        Thread.sleep(5000);
        System.out.println("" +System.currentTimeMillis());
        Collection<Job> jobs3 = jobManager.popJobs(TestJobUtil.DEFAULT_TOPIC, 100);
        Assert.assertEquals(40, jobs3.size());
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

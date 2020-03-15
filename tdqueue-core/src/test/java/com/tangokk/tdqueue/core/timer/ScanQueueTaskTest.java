package com.tangokk.tdqueue.core.timer;

import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.repository.BaseRedisRepoTest;
import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;
import com.tangokk.tdqueue.core.util.TestJobUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class ScanQueueTaskTest extends BaseRedisRepoTest {


    JobStateChecklist stateChecklist;

    ReadyQueue readyQueue;

    DelayQueue delayQueue;

    ScanQueueTask scanQueueTask;


    @Before
    public void init() {
        RedisConnection connection = getConnection();
        stateChecklist = new JobStateChecklist(connection);
        readyQueue = new ReadyQueue(connection);
        delayQueue = new DelayQueue(5, connection);
        scanQueueTask = new ScanQueueTask(delayQueue, readyQueue, stateChecklist);
    }


    @Test
    public void testProcessReadyJobs() throws InterruptedException {
        List<Job> jobList = new ArrayList<>();
        Map<String, Integer> stateMap = new HashMap<>();
        // a ready job
        String id1 = System.currentTimeMillis() + "1";
        Job job1 = TestJobUtil.createJob(id1, 0);
        jobList.add(job1);
        stateMap.put(job1.getKeyOfJob(), JobState.WAITING.index);
        String id2 = System.currentTimeMillis() + "2";
        Job job2 = TestJobUtil.createJob(id2, 1000);
        jobList.add(job2);
        stateMap.put(job2.getKeyOfJob(), JobState.WAITING.index);
        String id3 = System.currentTimeMillis() + "3";
        Job job3 = TestJobUtil.createJob(id3, 4000);
        jobList.add(job3);
        stateMap.put(job3.getKeyOfJob(), JobState.WAITING.index);
        jobList.forEach(Job::initReadyTime);
        delayQueue.pushJobs(jobList.toArray(new Job[0]));
        stateChecklist.setJobsState(stateMap);
        scanQueueTask.run();

        Integer state1 = stateChecklist.getJobState(job1.getKeyOfJob());
        Assert.assertEquals(JobState.READY.index, (int)state1);

        Collection<String> readyJobs1 = readyQueue.popReadyJobKeys(job1.getTopic(), 10);
        Assert.assertEquals(1, readyJobs1.size());

        Thread.sleep(1000);
        scanQueueTask.run();

        Integer state2 = stateChecklist.getJobState(job2.getKeyOfJob());
        Assert.assertEquals(JobState.READY.index, (int)state2);

        Collection<String> readyJobs2 = readyQueue.popReadyJobKeys(job1.getTopic(), 10);
        Assert.assertEquals(1, readyJobs2.size());

        Thread.sleep(4000);
        scanQueueTask.run();
        Integer state3 = stateChecklist.getJobState(job3.getKeyOfJob());
        Assert.assertEquals(JobState.READY.index, (int)state3);

        Collection<String> readyJobs3 = readyQueue.popReadyJobKeys(job1.getTopic(), 10);
        Assert.assertEquals(1, readyJobs3.size());



    }




}

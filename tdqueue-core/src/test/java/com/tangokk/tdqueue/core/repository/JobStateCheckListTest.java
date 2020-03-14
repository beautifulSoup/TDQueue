package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.Constant;
import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobStateCheckListTest extends BaseRedisRepoTest{


    JobStateChecklist jobStateChecklist;


    @Before
    public void init() {
        jobStateChecklist = new JobStateChecklist(getConnection());
    }

    @Test
    public void testSetAndGetAndRemoveJobState(){
        String jobKey = "DEFAULT:" +System.currentTimeMillis();
        jobStateChecklist.setJobState(jobKey, JobState.READY.index);
        Integer jobState = jobStateChecklist.getJobState(jobKey);
        Assert.assertEquals(JobState.READY.index, (int) jobState);
        jobStateChecklist.removeJobState(jobKey);
        Integer jobKeyRemoved = jobStateChecklist.getJobState(jobKey);
        Assert.assertNull(jobKeyRemoved);
    }


    @Test
    public void testCompareAndSet() {
        String jobKey = "DEFAULT:" +System.currentTimeMillis();
        jobStateChecklist.setJobState(jobKey, JobState.READY.index);
        boolean ret = jobStateChecklist.compareAndSet(jobKey, JobState.PROCESSING.index, JobState.READY.index);
        Integer jobState = jobStateChecklist.getJobState(jobKey);
        Assert.assertTrue(ret);
        Assert.assertEquals( JobState.PROCESSING.index, (int)jobState);
        boolean ret2 = jobStateChecklist.compareAndSet(jobKey, JobState.READY.index);
        Assert.assertFalse(ret2);
        jobStateChecklist.removeJobState(jobKey);
    }






}

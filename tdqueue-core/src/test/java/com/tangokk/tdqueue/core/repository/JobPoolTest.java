package com.tangokk.tdqueue.core.repository;


import com.tangokk.tdqueue.core.Constant;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.util.TestJobUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobPoolTest {


    JobPool jobPool;

    @Before
    public void init() {
        RedisConfiguration config = new RedisConfiguration(Constant.host, Constant.port, Constant.password, Constant.database);
        RedisConnection connection = new RedisConnection(config);
        jobPool = new JobPool(connection);
    }


    @Test
    public void testAddAndGetAndRemove() {
        Job job = TestJobUtil.createJob("" + System.currentTimeMillis(), 1000);
        job.initReadyTime();
        jobPool.addJob(job);
        Job jobGet = jobPool.getJob(job.getKeyOfJob());
        Assert.assertNotNull(jobGet);
        Assert.assertEquals(jobGet.getId(), job.getId());
        jobPool.removeJobs(job.getKeyOfJob());
        Job jobRemoved = jobPool.getJob(job.getKeyOfJob());
        Assert.assertNull(jobRemoved);
    }


}

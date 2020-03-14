package com.tangokk.tdqueue.core.repository;


import com.tangokk.tdqueue.core.Constant;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import com.tangokk.tdqueue.core.util.TestJobUtil;
import java.util.Collection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobBucketTest extends BaseRedisRepoTest{


    JobBucket bucket;

    @Before
    public void init() {
        bucket = new JobBucket(getConnection(), 10);
    }

    @Test
    public void pushAndPopTimeoutJob() throws InterruptedException {
        Job job = TestJobUtil.createJob("" + 11231, 1000);
        job.initReadyTime();
        bucket.pushJob(job);
        Collection<String> jobKeys = bucket.popTimeUpJobKeys();
        Assert.assertEquals(0, jobKeys.size());
        Thread.sleep(2000);
        Collection<String> afterDelay = bucket.popTimeUpJobKeys();
        Assert.assertEquals( 1, afterDelay.size());
    }



}

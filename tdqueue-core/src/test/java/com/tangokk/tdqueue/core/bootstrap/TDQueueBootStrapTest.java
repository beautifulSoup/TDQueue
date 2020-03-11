package com.tangokk.tdqueue.core.bootstrap;


import static com.tangokk.tdqueue.core.util.TestJobUtil.createJob;

import com.tangokk.tdqueue.core.Constant;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.service.JobManager;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TDQueueBootStrapTest {



    private static final String TOPIC = "DEFAULT";

    JobManager jobManager;


    @Before
    public void init() {
        RedisConfiguration configuration = new RedisConfiguration(Constant.host, Constant.port, Constant.password, Constant.database);
        jobManager = TDQueueBootStrap.bootstrap(configuration, 1);
    }


    @Test
    public void pushAndThenPop() throws InterruptedException {

        for(int i=0;i<50;i++) {
            Job job = createJob("" + i, 1000);
            jobManager.pushJob(job);
        }
        for(int i=0;i<50;i++) {
            Job job = createJob("" + (i+50), 2000);
            jobManager.pushJob(job);
        }

        Thread.sleep(1100);

        Collection<Job> jobs = jobManager.popJobs(TOPIC, 100);
        Assert.assertEquals(jobs.size(), 50);

        Thread.sleep(1000);
        Assert.assertEquals(jobs.size(), 50);

    }




}

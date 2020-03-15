package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.entity.Job;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReadyQueueTest extends BaseRedisRepoTest {

    ReadyQueue readyQueue;

    @Before
    public void init() {
        readyQueue = new ReadyQueue(getConnection());
    }



    @Test
    public void testPushAndPopReadyKey() {
        String topic = "DEFAULT";
        String jobKey = topic + ":" + System.currentTimeMillis();
        readyQueue.pushReadyJobKey(jobKey);
        Collection<String> readyKeys = readyQueue.popReadyJobKeys(topic, 10);
        Assert.assertEquals(1, readyKeys.size());
        Assert.assertEquals(jobKey, readyKeys.iterator().next());
    }


    @Test
    public void testPushAndPopReadyKeys() {
        String topic = "DEFAULT";
        List<String> jobKeys = new ArrayList<>();
        for(int i=0;i<100;i++) {
            jobKeys.add(Job.getJobKey(topic,  "" + System.currentTimeMillis() + i));
        }
        readyQueue.pushReadyJobKeys(jobKeys.toArray(new String[0]));
        Collection<String> readyKeys1 = readyQueue.popReadyJobKeys(topic, 50);
        Assert.assertEquals(50, readyKeys1.size());
        Collection<String> readyKeys2 = readyQueue.popReadyJobKeys(topic, 100);
        Assert.assertEquals(50, readyKeys2.size());
    }


}

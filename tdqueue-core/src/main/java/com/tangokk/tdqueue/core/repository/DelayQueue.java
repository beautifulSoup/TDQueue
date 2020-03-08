package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DelayQueue {


    List<JobBucket> jobBuckets;

    Integer bucketCount;

    RedisConnection redisConnection;

    public DelayQueue(Integer bucketCount, RedisConnection redisConnection) {
        this.bucketCount = bucketCount;
        this.redisConnection = redisConnection;
        initJobBucket(bucketCount);
    }


    private void initJobBucket(Integer bucketCount) {
        jobBuckets = new ArrayList<JobBucket>();
        for(int i=0;i<bucketCount;i++) {
            JobBucket bucket = new JobBucket(redisConnection, i);
            jobBuckets.add(bucket);
        }
    }

    public void pushJob(Job job) {
        Long index = job.getReadyTime() % bucketCount;
        jobBuckets.get(index.intValue()).pushJob(job);
    }

    public List<String> popTimeUpJobKeys()  {
        List<String> ret = new ArrayList<String>();
        ExecutorService executorService = Executors.newFixedThreadPool(bucketCount);
        List<ScanBucketTask> tasks = new ArrayList<ScanBucketTask>();
        for(int i=0;i<bucketCount;i++) {
            tasks.add(new ScanBucketTask(jobBuckets.get(i)));
        }
        List<Future<Collection<String>>> taskFutures;
        try {
             taskFutures = executorService.invokeAll(tasks);
            for(Future<Collection<String>> f : taskFutures) {
                Collection<String> result = f.get();
                ret.addAll(result);
            }
            return ret;
        } catch (ExecutionException  e) {
            log.error("wtf", e);
            return Collections.emptyList();
        } catch (InterruptedException e) {
            log.error("wtf", e);
            return Collections.emptyList();
        }

    }

    class ScanBucketTask implements Callable<Collection<String>> {

        JobBucket jobBucket;

        public ScanBucketTask(JobBucket bucket) {
            jobBucket = bucket;
        }

        public Collection<String> call() throws Exception {
            Collection<String> keys = jobBucket.popTimeUpJobKeys();
            return keys;
        }
    }





}

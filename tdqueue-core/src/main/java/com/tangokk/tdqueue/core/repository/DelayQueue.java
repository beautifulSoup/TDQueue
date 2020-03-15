package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.redis.RedisConnection;

import java.util.*;
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

    ExecutorService executorService;

    public DelayQueue(Integer bucketCount, RedisConnection redisConnection) {
        this.bucketCount = bucketCount;
        this.redisConnection = redisConnection;
        initJobBucket(bucketCount);
        executorService = Executors.newFixedThreadPool(bucketCount);
    }


    private void initJobBucket(Integer bucketCount) {
        jobBuckets = new ArrayList<>();
        for(int i=0;i<bucketCount;i++) {
            JobBucket bucket = new JobBucket(redisConnection, i);
            jobBuckets.add(bucket);
        }
    }

    public void pushJob(Job job) {
        long index = job.getReadyTime() % bucketCount;
        jobBuckets.get((int) index).pushJob(job);
    }

    public void pushJobs(Job[] jobs) {
        Map<Integer, List<Job>> groupToBucketMap = new HashMap<>();
        for(Job job:jobs) {
            Integer index = (int)(job.getReadyTime() % bucketCount);
            List<Job> bucketJobList = groupToBucketMap.computeIfAbsent(index, k -> new ArrayList<>());
            bucketJobList.add(job);
        }

        for(int i =0;i<bucketCount;i++) {
            List<Job> bucketJobList = groupToBucketMap.get(i);
            if(bucketJobList != null) {
                pushJobsToBucket(i, bucketJobList);
            }
        }

    }

    private void pushJobsToBucket(int index, List<Job> jobs) {
        jobBuckets.get(index).pushJobs(jobs.toArray(new Job[0]));
    }

    public List<String> popTimeUpJobKeys()  {
        List<String> ret = new ArrayList<String>();

        List<ScanBucketTask> tasks = new ArrayList<>();
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
        } catch (ExecutionException | InterruptedException e) {
            log.error("wtf", e);
            return Collections.emptyList();
        }

    }

    class ScanBucketTask implements Callable<Collection<String>> {

        JobBucket jobBucket;

        ScanBucketTask(JobBucket bucket) {
            jobBucket = bucket;
        }

        public Collection<String> call() {
            return jobBucket.popTimeUpJobKeys();
        }
    }





}

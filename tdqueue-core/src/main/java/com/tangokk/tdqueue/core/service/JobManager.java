package com.tangokk.tdqueue.core.service;

import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobPool;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;

import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobManager {


    JobPool jobPool;

    DelayQueue delayQueue;

    ReadyQueue readyQueue;

    JobStateChecklist jobStateChecklist;


    public JobManager(JobPool jobPool, DelayQueue delayQueue, ReadyQueue readyQueue, JobStateChecklist jobStateChecklist) {
        this.jobPool = jobPool;
        this.delayQueue = delayQueue;
        this.readyQueue = readyQueue;
        this.jobStateChecklist = jobStateChecklist;
    }

    public void pushJob(Job job) {
        if(job.getDelay() != null) {
            job.initReadyTime();
        }
        jobPool.addJob(job);
        if(job.getDelay() == null || job.getDelay() <=0 ) {  //don't need to delay, put in to the ready queue
            jobStateChecklist.setJobState(job.getKeyOfJob(), JobState.READY.index);
            readyQueue.pushReadyJobKey(job.getKeyOfJob());
        } else {
            jobStateChecklist.setJobState(job.getKeyOfJob(), JobState.WAITING.index);
            delayQueue.pushJob(job);
        }
    }

    public void pushJobs(Job [] jobs) {
        List<Job> delayJobs = new ArrayList<>();
        Map<String, Integer> delayStateMap = new HashMap<>();
        List<Job> readyJobs = new ArrayList<>();
        Map<String, Integer> readyStateMap = new HashMap<>();
        Arrays.stream(jobs)
                .forEach(j -> {
                    if(j.getDelay() == null || j.getDelay() <=0) {
                        readyJobs.add(j);
                        readyStateMap.put(j.getKeyOfJob(), JobState.READY.index);
                    } else {
                        delayJobs.add(j);
                        delayStateMap.put(j.getKeyOfJob(), JobState.WAITING.index);
                    }
                });
        jobPool.addJobs(jobs);
        jobStateChecklist.setJobsState(readyStateMap);
        readyQueue.pushReadyJobKeys(readyJobs.stream().map(Job::getKeyOfJob).toArray(String[]::new));
        jobStateChecklist.setJobsState(delayStateMap);
        delayQueue.pushJobs(delayJobs.toArray(new Job[0]));
    }


    /**
     * pop jobs from ready queue
     * @param topic the topic of jobs
     * @param count the count of jobs to pop
     * @return the jobs popped
     */
    public Collection<Job> popJobs(String topic, Integer count) {
        Collection<String> popedReadyJobKeys = readyQueue.popReadyJobKeys(topic, count);
        List<Job> poppedJobs = popedReadyJobKeys.stream()
            .filter(k -> ! jobStateChecklist.getJobState(k).equals(JobState.DELETED.index))  //filter jobs already been deleted
            .map(k -> jobPool.getJob(k))
            .collect(Collectors.toList());
        //go to processing
        poppedJobs.
            forEach(j -> {
                jobStateChecklist.setJobState(j.getKeyOfJob(), JobState.PROCESSING.index);
            });
        return poppedJobs;
    }


    /**
     * ack the job is processed
     *
     * @param topic the topic of job to ack
     * @param jobId the id of the job to ack
     * @return true ack process success, false ack process fail
     */
    public boolean ackJobProcessed(String topic, String jobId) {
        return jobStateChecklist.compareAndSet(Job.getJobKey(topic, jobId), JobState.FINISH.index, JobState.PROCESSING.index);
    }

    /**
     * cancel job by client only when job's state is ready or waiting.
     * If the job's state is processing or timeout or finish, it's can not be deleted
     *
     * @param topic the topic of the job to remove
     * @param jobId the id of the job to remove
     * @return true remove job success, false remove job fail
     */
    public boolean removeJob(String topic, String jobId) {
        return jobStateChecklist.compareAndSet(Job.getJobKey(topic, jobId), JobState.DELETED.index, JobState.READY.index, JobState.WAITING.index);
    }


}

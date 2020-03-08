package com.tangokk.tdqueue.core.service;

import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.entity.Job;
import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobPool;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;
import java.util.Collection;
import java.util.List;
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
            job.setReadyTime(System.currentTimeMillis() + job.getDelay());
        }
        jobPool.addJob(job);
        if(job.getDelay() == null || job.getDelay() <=0 ) {  //don't need to delay, put in to the ready queue
            jobStateChecklist.setJobState(job.getKeyOfJob(), JobState.READY.index);
            readyQueue.pushReadyJobKeys(job.getKeyOfJob());
        } else {
            jobStateChecklist.setJobState(job.getKeyOfJob(), JobState.WAITING.index);
            delayQueue.pushJob(job);
        }
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
     * @param jobKey the key of job
     * @return true ack process success, false ack process fail
     */
    public boolean ackJobProcessed(String jobKey) {
        return jobStateChecklist.compareAndSet(jobKey, JobState.FINISH.index, JobState.PROCESSING.index);
    }

    /**
     * cancel job by client only when job's state is ready or waiting.
     * If the job's state is processing or timeout or finish, it's can not be deleted
     *
     * @param job
     * @return true remove job success, false remove job fail
     */
    public boolean removeJob(Job job) {
        return jobStateChecklist.compareAndSet(job.getKeyOfJob(), JobState.DELETED.index, JobState.READY.index, JobState.WAITING.index);
    }


}

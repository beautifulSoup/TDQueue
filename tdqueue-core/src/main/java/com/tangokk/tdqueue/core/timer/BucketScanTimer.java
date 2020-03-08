package com.tangokk.tdqueue.core.timer;

import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BucketScanTimer {


    private ScheduledExecutorService service;


    public BucketScanTimer() {
        service = Executors.newSingleThreadScheduledExecutor();
    }


    public static BucketScanTimer getInstance() {
        return new BucketScanTimer();
    }


    /**
     * move keys of ready job from delay queue to ready queue
     * @param delayQueue
     * @param readyQueue
     * @param jobStateChecklist
     */
    public void startScan(DelayQueue delayQueue, ReadyQueue readyQueue, JobStateChecklist jobStateChecklist) {
        service.scheduleAtFixedRate(new ScanQueueTask(delayQueue, readyQueue, jobStateChecklist), 0, 1,
            TimeUnit.SECONDS);
    }


    private class ScanQueueTask implements Runnable {


        DelayQueue delayQueue;
        ReadyQueue readyQueue;
        JobStateChecklist jobStateChecklist;

        public ScanQueueTask(DelayQueue delayQueue, ReadyQueue readyQueue, JobStateChecklist jobStateChecklist){
            this.delayQueue = delayQueue;
            this.readyQueue = readyQueue;
            this.jobStateChecklist = jobStateChecklist;
        }


        @Override
        public void run() {
            try {
                List<String> timeUpKeys = delayQueue.popTimeUpJobKeys();
                processTimeUpJobs(timeUpKeys);
            } catch (Exception e) {
                log.error("Scan delay queue fail", e);
            }
        }


        private void processTimeUpJobs(Collection<String> timeUpKeys) {
            List<String> toReadyKeys = new ArrayList<>();
            timeUpKeys
                .forEach(k -> {
                    Integer state = jobStateChecklist.getJobState(k);
                    if(state == JobState.WAITING.index) {
                        toReadyKeys.add(k);
                    } else if(state == JobState.DELETED.index){
                        log.info("Job has been deleted: " + k);
                        //Do nothing
                    } else if(state == JobState.PROCESSING.index) {
                        //job is being processed, process timeout
                        //TODO post process the timeout jobs
                        log.error("Job processing timeout: " + k);
                        jobStateChecklist.setJobState(k, JobState.TIMEOUT.index);
                    }
                });

            readyQueue.pushReadyJobKeys(toReadyKeys);
            timeUpKeys.forEach(k ->
                jobStateChecklist.setJobState(k, JobState.READY.index)
            );
        }


    }


}

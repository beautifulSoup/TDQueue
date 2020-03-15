package com.tangokk.tdqueue.core.timer;

import com.tangokk.tdqueue.core.constant.JobState;
import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class ScanQueueTask implements Runnable {


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
            Map<String, Integer> jobStateMap = new HashMap<>();
            timeUpKeys
                .forEach(k -> {
                    Integer state = jobStateChecklist.getJobState(k);
                    if(state == null) {
                        log.error("wtf: state of job - {} is null", k);
                    } else if(state == JobState.WAITING.index) {
                        toReadyKeys.add(k);
                        jobStateMap.put(k, JobState.READY.index);
                    } else if(state == JobState.DELETED.index){
                        log.info("Job has been deleted: " + k);
                        //Do nothing
                    } else if(state == JobState.PROCESSING.index) {
                        //job is being processed, process timeout
                        //TODO post process the timeout jobs
                        log.error("Job processing timeout: " + k);
                        jobStateMap.put(k, JobState.TIMEOUT.index);
                    }
                });

            readyQueue.pushReadyJobKeys(toReadyKeys.toArray(new String[0]));
            jobStateChecklist.setJobsState(jobStateMap);

        }


    }

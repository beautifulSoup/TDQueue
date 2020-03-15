package com.tangokk.tdqueue.core.timer;

import com.tangokk.tdqueue.core.repository.DelayQueue;
import com.tangokk.tdqueue.core.repository.JobStateChecklist;
import com.tangokk.tdqueue.core.repository.ReadyQueue;
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
        log.info("Delay queue scan: " +System.currentTimeMillis());
        service.scheduleAtFixedRate(new ScanQueueTask(delayQueue, readyQueue, jobStateChecklist), 0, 1,
            TimeUnit.SECONDS);
    }




}

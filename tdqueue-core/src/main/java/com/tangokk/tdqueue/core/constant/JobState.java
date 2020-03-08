package com.tangokk.tdqueue.core.constant;

public enum JobState {


    WAITING(0),
    READY(1),
    PROCESSING(2),
    FINISH(3),
    DELETED(4),
    TIMEOUT(5);

    public int index;

    JobState(Integer i) {
        index = i;
    }



}

package com.tangokk.tdqueue.core.util;

import com.tangokk.tdqueue.core.entity.Job;

public class TestJobUtil {


    public static final String DEFAULT_TOPIC = "DEFAULT";

    public static Job createJob(String id, long delay) {
        return Job.builder()
            .topic(DEFAULT_TOPIC)
            .id(id)
            .delay(delay)
            .body("1")
            .ttr(15000l)
            .build();
    }

}

package com.tangokk.tdqueue.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class Job {

    public static final String SPLIT_CHAR = ":";

    /**
     * topic of job
     */
    String topic;

    /**
     * id of job
     */
    String id;

    /**
     * time to delay in millseconds
     */
    Long delay;

    /**
     * timeout to execute
     */
    Long ttr;


    /**
     * job data
     */
    String body;

    /**
     * ready time in millseconds
     */
    Long readyTime;

    /**
     *
     * @return the key that this job stored in redis
     */
    @JsonIgnore
    public String getKeyOfJob() {
        return topic + SPLIT_CHAR + id;
    }


    @JsonIgnore
    public static String getJobKey(String topic, String jobId) {
        return topic + SPLIT_CHAR + jobId;
    }


    @JsonIgnore
    public static String getTopicOfJobKey(String jobKey) {
        return jobKey.split(SPLIT_CHAR)[0];
    }


}

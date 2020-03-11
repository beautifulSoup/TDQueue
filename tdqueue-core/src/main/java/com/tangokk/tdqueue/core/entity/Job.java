package com.tangokk.tdqueue.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Builder
@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
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
    private Long readyTime;

    public void initReadyTime() {
        if(delay == null) {
            log.error("Init ready time fail, the dalay is null");
        }
        readyTime = System.currentTimeMillis() + delay;
    }


    public Long getReadyTime() {
        return readyTime;
    }



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

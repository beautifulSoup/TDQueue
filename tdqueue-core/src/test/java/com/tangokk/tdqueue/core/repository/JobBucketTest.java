package com.tangokk.tdqueue.core.repository;


import com.tangokk.tdqueue.core.Constant;
import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.redis.RedisConnection;
import org.junit.Before;
import org.junit.Test;

public class JobBucketTest {


    JobBucket bucket;

    @Before
    public void init() {
        RedisConfiguration configuration = new RedisConfiguration(Constant.host, Constant.port, Constant.password, Constant.database);
        RedisConnection redisConnection = new RedisConnection(configuration);
        bucket = new JobBucket(redisConnection, 0);
    }

    @Test
    public void popTimeUpJobKeys() {
        bucket.popTimeUpJobKeys();
    }

    @Test
    public void pushJob() {
    }
}

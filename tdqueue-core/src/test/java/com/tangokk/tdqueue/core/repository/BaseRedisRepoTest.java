package com.tangokk.tdqueue.core.repository;

import com.tangokk.tdqueue.core.Constant;
import com.tangokk.tdqueue.core.redis.RedisConfiguration;
import com.tangokk.tdqueue.core.redis.RedisConnection;

public class BaseRedisRepoTest {


    protected RedisConnection getConnection() {
        RedisConfiguration configuration = new RedisConfiguration(Constant.host, Constant.port, Constant.password, Constant.database);
        return new RedisConnection(configuration);
    }
}

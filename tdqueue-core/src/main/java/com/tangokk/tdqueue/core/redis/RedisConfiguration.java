package com.tangokk.tdqueue.core.redis;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RedisConfiguration {

    String host;

    Integer port;

    String password;

    Integer database;

}

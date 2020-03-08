package com.tangokk.tdqueue.core.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

public class JsonUtil {

    private static ObjectMapper om = new ObjectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);


    public static<T> T parseJson(String json, Class<T> clz) throws JsonProcessingException {
        return om.readValue(json, clz);
    }


    public static String generateJson(Object obj) throws JsonProcessingException {
        return om.writeValueAsString(obj);
    }

}

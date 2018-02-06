package com.stream.KSH;

import org.json.JSONObject;

/**
 * Created by adrien on 15/12/17.
 */
public class JsonTest {
    public static void main(String[] args) {
        String line = "{\"@timestamp\":\"2017-12-14T18:52:52.242+01:00\", \"@version\":1,\"message\":\"Hbase is cool\", \"logger_name\":\"net.logstash.log4j.LogProducer\", \"thread_name\":\"main\",\"level\":\"TRACE\", \"level_value\":5000, \"HOSTNAME\":\"adrien-MS-7A72\"}";
        JSONObject jobj = new JSONObject(line);
        System.out.println(jobj.get("@timestamp").toString());
        System.out.println(jobj.get("@version").toString());
        System.out.println(jobj.get("message").toString());
        System.out.println(jobj.get("logger_name").toString());
        System.out.println(jobj.get("thread_name").toString());
        System.out.println(jobj.get("level").toString());
        System.out.println(jobj.get("level_value").toString());
        System.out.println(jobj.get("HOSTNAME").toString());


    }
}

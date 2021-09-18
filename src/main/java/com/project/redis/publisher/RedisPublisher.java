package com.project.redis.publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RedisPublisher {

    @Autowired
    private RedisTemplate< String, String > template;

    @Scheduled( fixedDelay = 100 )
    public void publish() {
        ObjectRecord<String, String> record = StreamRecords.newRecord()
                .ofObject("movieDetails")
                .withStreamKey("streamKey");
        template
                .opsForStream()
                .add(record);
    }
}
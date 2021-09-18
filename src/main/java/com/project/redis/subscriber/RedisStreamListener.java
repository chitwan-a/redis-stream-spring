package com.project.redis.subscriber;
import lombok.SneakyThrows;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RedisStreamListener implements StreamListener<String, ObjectRecord<String, String>> {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @PostConstruct
    public void init() {
        try {
            this.redisTemplate.opsForStream().createGroup("streamKey", "streamKeyGroup");
        } catch (InvalidDataAccessApiUsageException e) {
                var cause = e.getCause();
                if (cause != null ) {//&& RedisBusyException.class.equals(cause.getClass())) {
                    System.out.println("STREAM - Redis group already exists, skipping Redis group creation: ");
                } else throw e;
            }
    }

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, String> record) {

        System.out.println(" - consumed :" + record.getValue());

        this.redisTemplate
                    .opsForZSet()
                    .incrementScore(record.getValue(), "MovieDetails", 1);

        atomicInteger.incrementAndGet();
    }
}

package kcp.data.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "users", groupId = "group_id")
    public void consume(String message) throws IOException {
        System.out.println(String.format("#### -> Consumed message -> %s", message));
    }

}


// source data
// oracle, mysql, tomcat server occur logging
// oracle -> PG Table number 1,000


// spark opensource


// Hadoop -> HDFS FileSystem save

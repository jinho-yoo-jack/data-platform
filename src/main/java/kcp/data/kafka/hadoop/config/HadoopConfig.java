package kcp.data.kafka.hadoop.config;


import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class HadoopConfig {

    @Bean
    public Configuration makeConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        return configuration;
    }


}

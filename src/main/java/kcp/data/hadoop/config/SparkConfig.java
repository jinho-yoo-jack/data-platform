package kcp.data.hadoop.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String master;

    @Bean
    public SparkSession sparkSession(){
        return SparkSession
            .builder()
            .appName(appName)
            .master(master)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate();
    }
}

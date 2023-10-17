package kcp.data.hadoop.config;


import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.util.concurrent.TimeUnit;

@Configuration
public class HadoopConfig {
    private static final String BASE_URL = "http://localhost:50070";

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration(){
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration(); // conf config cfg
        config.addResource("/Users/black/dev/bigdata/hadoop-3.3.5/etc/hadoop/core-site.xml");
        config.addResource("/Users/black/dev/bigdata/hadoop-3.3.5/etc/hadoop/hdfs-site.xml");
        config.set("fs.hdfs.impl",
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() // DFS
        );
        config.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        return config;
    }

    @Bean
    public OkHttpClient okHttpClient() {
        return new OkHttpClient.Builder().connectTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .build();
    }

    @Bean
    public Retrofit retrofit(OkHttpClient client) {
        return new Retrofit.Builder().baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create())
            .client(client)
            .build();
    }

    @Bean
    public WebHdfsService webHdfsAPIs(Retrofit retrofit) {
        return retrofit.create(WebHdfsService.class);
    }

}

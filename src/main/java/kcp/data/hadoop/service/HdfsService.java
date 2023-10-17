package kcp.data.hadoop.service;

import kcp.data.hadoop.config.SparkConfig;
import kcp.data.hadoop.config.WebHdfsService;
import kcp.data.hadoop.dto.RespFileStatuses;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
@RequiredArgsConstructor
public class HdfsService {
    private final WebHdfsService webHdfsService;
    private final SparkConfig sparkConfig;

    @Value("${hadoop.hdfs.url}")
    private String hdfsPath;

    private String dbUser;
    private String dbPassword;

    public Dataset<Row> getData2Source() {
        SparkSession spark = sparkConfig.sparkSession();
        String dbUrl = "";
        String dbtable = "(select c1, c2 from t1) as subq";
        String partitionColumn = "";
        int numPartitions = 5;
        return sparkConfig.sparkSession().read()
            .format("jdbc")
            .option("url", dbUrl)
            .option("user", dbUser)
            .option("password", dbPassword)
            .option("dbtable", dbtable)
            .option("partitionColumn", partitionColumn)
            .option("lowerBound", "1")
            .option("upperBound", "100")
            .option("numPartitions", numPartitions)
            .load();
    }

    public void save2DestDb(Dataset<Row> dataSource, String toPath) {
        dataSource.write().mode("overwrite").parquet(toPath);
    }

    public boolean isExistedDirectory(String toPath) throws IOException {
        Optional<RespFileStatuses> response = Optional.ofNullable(webHdfsService.getListDirectory(toPath).execute().body());
        return response.isPresent();
    }

    public boolean createDirectory(String toPath) throws IOException {
        return Optional.ofNullable(webHdfsService.createNewDirectory(toPath).execute().body())
            .orElse(new HashMap<String, Boolean>())
            .getOrDefault("boolean", false);
    }

    public RespFileStatuses getListDirectory(String toPath) throws IOException {
        return webHdfsService.getListDirectory(toPath).execute().body();
    }
}

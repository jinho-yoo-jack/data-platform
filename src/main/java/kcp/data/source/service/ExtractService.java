package kcp.data.source.service;

import kcp.data.hadoop.config.HadoopConfig;
import kcp.data.hadoop.config.SparkConfig;
import kcp.data.source.dto.request.ReqSrcTableExtract;
import kcp.data.source.dto.response.RespSrcTableExtract;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class ExtractService {
    private final SparkConfig sparkConfig;
    private final HadoopConfig hadoopConfig;

    @Value("${jdbc:oracle:thin:@localhost}")
    private String dbUrl;
    private String hdfsUrl;

    public RespSrcTableExtract getSourceData(ReqSrcTableExtract reqMessage) throws IOException, URISyntaxException {
        Dataset<Row> rows = getTableData(reqMessage);
        String path = "/tmp/" + reqMessage.getTableName();
        saveToHdfs(rows, path);
        if(fileExists(path))
            return new RespSrcTableExtract(reqMessage.getTableName(), path, true, LocalDateTime.now());

        return new RespSrcTableExtract(reqMessage.getTableName(), path, false, LocalDateTime.now());
    }

    public Dataset<Row> getTableData(ReqSrcTableExtract reqMessage) throws IOException {
//        String dbUrl = "jdbc:oracle:thin:@localhost:1521:XE";
        return sparkConfig.sparkSession().read()
            .format("jdbc")
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("url", dbUrl)
            .option("user", "ot")
            .option("password", "oracle")
            .option("dbtable", reqMessage.getTableName())
            .option("partitionColumn", reqMessage.getPartitionColumnName())
            .option("numPartitions", reqMessage.getNumPartitions())
            .option("lowerBound", "1")
            .option("upperBound", "100")
            .load();
    }

    public void saveToHdfs(Dataset<Row> rows, String path){
        rows.write().format("parquet").mode("overwrite").save(path);
    }

    public boolean fileExists(String hdfsPath) throws URISyntaxException, IOException {
        Configuration config = hadoopConfig.hadoopConfiguration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), config);
        Path createdPath = new Path(hdfsPath + "/_SUCCESS");
        return hdfs.exists(createdPath);
    }

}

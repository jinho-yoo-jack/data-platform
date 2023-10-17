package kcp.data;

import kcp.data.dto.UserRank;
import kcp.data.hadoop.config.HadoopConfig;
import kcp.data.hadoop.config.SparkConfig;
import kcp.data.hadoop.config.WebHdfsService;
import kcp.data.hadoop.dto.RespFileStatuses;
import kcp.data.hadoop.service.HdfsService;
import kcp.data.source.dto.request.SourceSchema;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.net.*;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
@Slf4j
public class HdfsServiceTest {

    @Autowired
    private WebHdfsService webHdfsService;
    @Autowired
    private HdfsService hdfsService;
    @Autowired
    private HadoopConfig hadoopConfig;
    @Autowired
    private SparkConfig sparkConfig;

    private Configuration config;
    private SparkSession spark;

    @BeforeAll
    public void initTest() {
        this.config = hadoopConfig.hadoopConfiguration();
        this.spark = sparkConfig.sparkSession();
    }

    @Test
    public void getDataSourceTest() {
        // The spring boot only serve to import source data.
        // Data format modification is carried out in the post-processing phase.
        // Request Message Format
        // - databaseName.tableName|partitionColumn|numPartitions
        String dbUrl = "jdbc:oracle:thin:@localhost:1521:XE";
        String dbtable = "REGIONS";
        String partitionColumn = "REGION_ID";
        int numPartitions = 5; // number worker

        Dataset<Row> rows = sparkConfig.sparkSession().read()
            .format("jdbc")
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("url", dbUrl)
            .option("user", "ot")
            .option("password", "oracle")
            .option("dbtable", dbtable)
            .option("partitionColumn", partitionColumn)
            .option("lowerBound", "1")
            .option("upperBound", "100")
            .option("numPartitions", numPartitions)
            .load();

        rows.printSchema();
        rows.write().mode("overwrite").parquet("hdfs://localhost:9000/users/region.parquet");
        rows.show();
        System.out.println(rows.schema());
    }

    @Test
    public void sparkTest() {
        SourceSchema<UserRank> schema = new SourceSchema<>(UserRank.class);
        // kafka message insert
        Dataset<Row> df = spark.createDataFrame(schema.getReceiveMessages(), schema.getSchema());
        df.show();

        String toPath = "hdfs://localhost:9000/pg_4.parquet";
        df.write().mode("overwrite").parquet(toPath);
    }

    @Test
    public void getHANameNodeServiceId() {
        Map<String, Map<String, InetSocketAddress>> result = DFSUtilClient.getHaNnRpcAddresses(config);
        log.info("[RESULT] NameNodes Service Id ::: {}", result);
    }

    public FileSystem getHdfs(String nnURI) throws URISyntaxException, IOException {
        URI hdfsURI = new URI("hdfs://" + nnURI);
        return FileSystem.get(hdfsURI, config);
    }

    public boolean createEmptyFile(FileSystem hdfs, String toPath) throws IOException {
        return hdfs.createNewFile(new Path(toPath));
    }

    public ObjectOutputStream getOutputStreamBuffer(FileSystem hdfs, Path toPath, boolean overwrite) throws IOException {
        FSDataOutputStream fsDataOutputStream = hdfs.create(toPath, true);
        ObjectOutputStream outputStream = new ObjectOutputStream(fsDataOutputStream);
        return outputStream;

//        return new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
    }

    public void writeDataInFile(FileSystem hdfs, String toPath, KafkaMessage inputData) throws IOException {
        writeDataInFile(hdfs, toPath, false, inputData);
    }

    public void writeDataInFile(FileSystem hdfs, String toPath, boolean overwrite, KafkaMessage inputData) throws IOException {
        Path hdfsWritePath = new Path(toPath);
        ObjectOutputStream stream = getOutputStreamBuffer(hdfs, hdfsWritePath, overwrite);
        stream.writeObject(inputData);
        stream.close();
    }

    @Test
    public void insertData() {

    }

    static class KafkaMessage implements Serializable {
        private String uuid;
        private HashMap<String, Integer> data;

        public KafkaMessage(String u, HashMap<String, Integer> d) {
            uuid = u;
            data = d;
        }

        public Integer getData(String key) {
            return data.get(key);
        }

        public void setData(String key, int value) {
            data.put(key, value);

        }

        public String getUuid() {
            return this.uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }
    }

    /*
     * uuid
     * pg_trade : price(long)
     *
     * */
    /*public Schema createAvroSchema() {
        Schema pgTradeSchema = SchemaBuilder.record("pg_data")
            .namespace("kcp.data")
            .fields()
            .requiredLong("price")
            .endRecord();

        return SchemaBuilder.record("kafkaMessage")
            .namespace("kcp.data")
            .fields()
            .name("pg_trade")
            .type()
            .array()
            .items()
            .type(pgTradeSchema)
            .noDefault().endRecord();
    }

    private List<GenericData.Record> getRecords(Schema schema) {
        List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
        GenericData.Record record = new GenericData.Record(schema);
        // Adding 2 records
        record.put("userId", 1);
        record.put("rank", 1);
        recordList.add(record);

        record = new GenericData.Record(schema);
        record.put("userId", 2);
        record.put("rank", 2);
        recordList.add(record);

        return recordList;
    }

    public void insert() throws IOException {
        Schema schema = SchemaBuilder.record("user_rank")
            .namespace("kcp.data")
            .fields()
            .requiredInt("userId")
            .requiredLong("rank")
            .endRecord();

        GenericRecord datum = new GenericData.Record(schema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer =
            new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

    }*/


    /*@Test
    public void createFileAndWriteToString() {
        String writeFilePath = "hdfs://localhost:9000/pg_20230908.parquet";
        Path toPath = new Path(writeFilePath);
        Schema schema = SchemaBuilder.record("user_rank")
            .namespace("kcp.data")
            .fields()
            .requiredInt("userId")
            .requiredLong("rank")
            .endRecord();

        try {
            ParquetWriter<GenericData.Record> writer = AvroParquetWriter.
                <GenericData.Record>builder(toPath)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema)
                .withConf(config)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(true)
                .withDictionaryEncoding(true)
                .build();

            for (GenericData.Record record : getRecords(schema)) {
                writer.write(record);
            }
            writer.close();

        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }*/

    @Test
    public void insertTest() {

    }


    @Test
    public void HadoopConfigureTest() throws IOException {

        Configuration config = hadoopConfig.hadoopConfiguration();

        try {
            FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), config);
            boolean isExistFile = hdfs.exists(new Path("/users/region.parquet/_SUCCESS"));


//            System.out.println(hdfs.getWorkingDirectory() + " this is from /n/n");
//            hdfs.createNewFile(new Path("hdfs://localhost:9000/newJinho"));
//            hdfs.create(new Path("/jinho1"));

//            String fileName = "20230907.txt";
//            Path hdfsWritePath = new Path("/jinho/" + fileName);
//            HashMap<String, Integer> data = new HashMap<>();
//            data.put("card_trade", 100000);
//
//            FSDataOutputStream fout = hdfs.create(hdfsWritePath);
//            ObjectOutputStream so = new ObjectOutputStream(fout);
//            so.writeObject(data);
//            so.close();
//            fout.close();
//            hdfs.close();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        FileSystem dfs = FileSystem.get(config);
        String dirName = "TestDirectory";
    }

    @Test
    public void getListDirectoryTest() {
        String directoryPath = "jinho";
        try {
            if (!hdfsService.isExistedDirectory(directoryPath)) {
                boolean isSuccess = hdfsService.createDirectory(directoryPath);
                if (!isSuccess) throw new IOException();
            }
            RespFileStatuses dirList = hdfsService.getListDirectory(directoryPath);
            log.info("[RESULT] GetListDirectory ::: {}", dirList);

        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    @Test
    public void createEmptyFile() throws IOException {
        String dirPath = "jinho";
        String fileName = "namenode.log";
        String delimiters = "/";
        String includePathFileName = joinAandBToDelimiters(dirPath, delimiters, fileName);

        try {
            Headers responseHeaders = webHdfsService.createEmptyNewFile(includePathFileName).execute().headers();
            String createdFilePath = responseHeaders.get("Location");
            log.info("[RESULT] createdFilePath ::: {}", createdFilePath);
        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    public String joinAandBToDelimiters(String a, String delimiters, String b) {
        return a + delimiters + b;
    }

    @Test
    public void writeMessage() {
        String dirPath = "jinho";
        String fileName = "namenode.log";
        String delimiters = "/";
        String includePathFileName = joinAandBToDelimiters(dirPath, delimiters, fileName);
        String text = "some text";
        RequestBody requestBody = RequestBody.create(MediaType.parse("text/plain"), text);
        try {
            webHdfsService.insertDataToFile(includePathFileName, requestBody).execute().body();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}


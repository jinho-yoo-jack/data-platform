package kcp.data.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
@Slf4j
public class HdfsServiceTest {

    @Autowired
    private Configuration configuration;

    @Test
    public void createDirectoryTest() {
        try {
            Configuration configuration1 = new Configuration();
            configuration1.set("fs.defaultFS", "file:///localhost:9000");
            FileSystem fileSystem = FileSystem.get(configuration1);
            String directoryName = "testDir/kcp";
            Path path = new Path(directoryName);
            boolean result = fileSystem.mkdirs(path);
            System.out.println("JHY RESULT : " + result);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

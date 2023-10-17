package kcp.data.hadoop.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;

@Data
public class RespFileStatuses {
    private final FileStatuses FileStatuses;

    @Data
    static class FileStatuses {
        private ArrayList<FileStatus> FileStatus;
    }

    @Data
    public static class FileStatus {
        private String pathSuffix;
        private String type;
    }
}

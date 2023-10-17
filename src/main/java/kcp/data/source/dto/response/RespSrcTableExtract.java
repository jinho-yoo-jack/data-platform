package kcp.data.source.dto.response;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;

@Data
@RequiredArgsConstructor
public class RespSrcTableExtract {
    private final String tableName;
    private final String path;
    private final boolean isSuccess;
    private final LocalDateTime execTime;
}

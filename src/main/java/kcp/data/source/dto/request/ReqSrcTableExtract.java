package kcp.data.source.dto.request;

import lombok.Data;

@Data
public class ReqSrcTableExtract {
    private String tableName; // OT.REGIONS
    private String partitionColumnName; // PK
    private String path;
    private int numPartitions; // 5
}

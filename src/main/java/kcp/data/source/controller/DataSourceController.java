package kcp.data.source.controller;

import kcp.data.common.response.ResponseMessage;
import kcp.data.source.dto.request.ReqSrcTableExtract;
import kcp.data.source.dto.response.RespSrcTableExtract;
import kcp.data.source.service.ExtractService;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URISyntaxException;

@RestController
@RequestMapping("/aml")
@RequiredArgsConstructor
public class DataSourceController {
    private final ExtractService extractService;
    @GetMapping("/extract")
    public ResponseMessage<RespSrcTableExtract> extract(@RequestBody ReqSrcTableExtract request) throws IOException, URISyntaxException {
        Dataset<Row> df = extractService.getTableData(request);
        extractService.saveToHdfs(df, "/users/"+request.getPath());
        return ResponseMessage.createSuccessRespMessage(extractService.getSourceData(request));
    }
}

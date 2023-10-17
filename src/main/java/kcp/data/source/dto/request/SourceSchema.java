package kcp.data.source.dto.request;

import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;

@Getter
public class SourceSchema<T> {
    private final List<T> receiveMessages;
    private final Class<T> schemaType;

    public SourceSchema(Class<T> s) {
        this(s, new ArrayList<>());
    }

    public SourceSchema(Class<T> s, List<T> r) {
        schemaType = s;
        receiveMessages = r;
    }

    public boolean add(T row) {
        return receiveMessages.add(row);
    }

    public boolean add(List<T> rows) {
        return receiveMessages.addAll(rows);
    }

    public void save(Dataset<Row> dataframe, String toPath) {
        dataframe.write().parquet(toPath);
    }

    public Class<T> getSchema() {
        return schemaType;
    }
}

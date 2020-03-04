package com.hedera.mirror.importer.parser.record;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import java.util.Map;
import javax.inject.Named;
import com.google.common.base.Stopwatch;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Named
public class BigQueryWriter {
    private final BigQueryWriterProperties properties;
    private final TableId tableId;
    private InsertAllRequest.Builder insertRequest;
    private int numRows;

    public BigQueryWriter(BigQueryWriterProperties bigQueryWriterProperties) {
        properties = bigQueryWriterProperties;
        tableId = TableId.of(properties.getProjectName(), properties.getDatasetName(), properties.getTableName());
        insertRequest = InsertAllRequest.newBuilder(tableId);
        numRows = 0;
    }

    public void insertRow(String id, Map<String, Object> bqRow) {
        insertRequest.addRow(id, bqRow);
        numRows++;
        if (numRows == properties.getBatchSize()) {
            sendToBigQuery();
        }
    }

    private void sendToBigQuery() {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Stopwatch stopwatch = Stopwatch.createStarted();
        InsertAllRequest insertAllRequest = insertRequest.build();
        log.info("Built InsertAllRequest in {}", stopwatch.elapsed());
        stopwatch.reset().start();
        InsertAllResponse response = bigquery.insertAll(insertRequest.build());
        log.info("Executed insertAll in {}", stopwatch.elapsed());
        if (response.hasErrors()) {
            log.info("Inserted {}/{} rows into transactions table. Errors below:", numRows - response.getInsertErrors().size(), numRows);
            // If any of the insertions failed, this lets you inspect the errors
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                for (var err : entry.getValue()) {
                    log.error("insert error : {} : {}", entry.getKey(), err);
                }
            }
        } else {
            log.info("Inserted {} rows into transactions table", numRows);
        }

        // Reset
        insertRequest = InsertAllRequest.newBuilder(tableId);
        numRows = 0;
    }
}

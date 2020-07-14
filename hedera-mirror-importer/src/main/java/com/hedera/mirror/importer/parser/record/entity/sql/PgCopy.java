package com.hedera.mirror.importer.parser.record.entity.sql;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2020 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.CaseFormat;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.log4j.Log4j2;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import com.hedera.mirror.importer.exception.ParserException;

/**
 * Stateless writer to insert rows into Postgres table using COPY.
 *
 * @param <T> domain object
 */
@Log4j2
public class PgCopy<T> {
    private final DataSource dataSource;
    private final String tableName;
    private final String columnsCsv;
    private final ObjectWriter writer;
    private final Timer buildCsvDurationMetric;
    private final Timer copyDurationMetric;
    private CopyManager copyManager;

    public PgCopy(DataSource dataSource, Class<T> tClass, MeterRegistry meterRegistry) {
        this.dataSource = dataSource;
        tableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tClass.getSimpleName());
        var mapper = new CsvMapper();
        var schema = mapper.schemaFor(tClass);
        writer = mapper.writer(schema);
        columnsCsv = Lists.newArrayList(schema.iterator()).stream()
                .map(CsvSchema.Column::getName)
                .map(name -> CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name))
                .collect(Collectors.joining(", "));
        buildCsvDurationMetric = Timer.builder("hedera.mirror.importer.parser.record.entity.sql.pgcopy.csv")
                .description("Time to build csv string")
                .tag("table", tableName)
                .register(meterRegistry);
        copyDurationMetric = Timer.builder("hedera.mirror.importer.parser.record.entity.sql.pgcopy.copy")
                .description("Time to insert transactions into table")
                .tag("table", tableName)
                .register(meterRegistry);
    }

    public void copy(List<T> items) {

        if (items == null || items.size() == 0) {
            return;
        }
        try {
            if (copyManager == null) {
                copyManager = dataSource.getConnection().unwrap(PGConnection.class).getCopyAPI();
            }

            Stopwatch stopwatch = Stopwatch.createStarted();
            var csv = buildCsv(items);
            var csvDuration = stopwatch.elapsed();
            buildCsvDurationMetric.record(csvDuration);

            log.debug("Copying {} rows to {} table", items.size(), tableName);
            long rowsCount = copyManager.copyIn(
                    String.format("COPY %s(%s) FROM STDIN WITH CSV", tableName, columnsCsv),
                    new StringReader(csv));
            copyDurationMetric.record(stopwatch.elapsed().minus(csvDuration));
            log.info("Copied {} rows to {} table in {}ms",
                    rowsCount, tableName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (IOException | SQLException e) {
            log.error(e);
            throw new ParserException(e);
        }
    }

    private String buildCsv(List<T> items) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        var csvData = writer.writeValueAsString(items);
        log.trace("{} table: csv string length={} time={}ms", tableName, csvData.length(),
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return csvData;
    }
}


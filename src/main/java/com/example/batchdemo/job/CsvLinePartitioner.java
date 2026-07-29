package com.example.batchdemo.job;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Basic feature: Partitioner - splits the CSV into contiguous line ranges so a
 * chunk-oriented step can process them concurrently, one partition per worker
 * thread. Each range is expressed as [startItem, endItem) and stored in the
 * partition's own ExecutionContext, which the step-scoped FlatFileItemReader
 * reads back via #{stepExecutionContext[...]} to bound its currentItemCount /
 * maxItemCount. The line count is scanned once, in the constructor (this bean
 * is a singleton), with a lazily-evaluated line stream - so the file is never
 * loaded into memory all at once, and isn't rescanned on every job run.
 */
public class CsvLinePartitioner implements Partitioner {

    private final int totalDataLines;

    public CsvLinePartitioner(Resource resource, int linesToSkip) {
        this.totalDataLines = countDataLines(resource, linesToSkip);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int linesPerPartition = (int) Math.ceil((double) totalDataLines / gridSize);

        Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
        int start = 0;
        int partitionNumber = 0;
        while (start < totalDataLines) {
            int end = Math.min(start + linesPerPartition, totalDataLines);

            ExecutionContext context = new ExecutionContext();
            context.putInt("startItem", start);
            context.putInt("endItem", end);
            context.putString("partitionName", "partition" + partitionNumber);
            partitions.put("partition" + partitionNumber, context);

            start = end;
            partitionNumber++;
        }
        return partitions;
    }

    private static int countDataLines(Resource resource, int linesToSkip) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
            return (int) reader.lines().skip(linesToSkip).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

package com.example.batchdemo.job;

import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for CsvLinePartitioner's line-range math, independent of the real
 * people.csv file or a running Spring context.
 */
class CsvLinePartitionerTest {

    private static Resource csvOf(String... dataLines) {
        String content = "header\n" + String.join("\n", dataLines) + "\n";
        return new ByteArrayResource(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void splitsDataLinesIntoContiguousRangesCoveringTheWholeFile() {
        CsvLinePartitioner partitioner = new CsvLinePartitioner(csvOf("a", "b", "c", "d", "e", "f", "g"), 1);

        Map<String, ExecutionContext> partitions = partitioner.partition(3);

        assertThat(partitions).hasSize(3);
        assertThat(partitions.get("partition0").getInt("startItem")).isEqualTo(0);
        assertThat(partitions.get("partition0").getInt("endItem")).isEqualTo(3);
        assertThat(partitions.get("partition1").getInt("startItem")).isEqualTo(3);
        assertThat(partitions.get("partition1").getInt("endItem")).isEqualTo(6);
        assertThat(partitions.get("partition2").getInt("startItem")).isEqualTo(6);
        assertThat(partitions.get("partition2").getInt("endItem")).isEqualTo(7);
    }

    @Test
    void producesFewerPartitionsThanGridSizeWhenThereAreFewerDataLines() {
        CsvLinePartitioner partitioner = new CsvLinePartitioner(csvOf("a", "b"), 1);

        Map<String, ExecutionContext> partitions = partitioner.partition(5);

        assertThat(partitions).hasSize(2);
        assertThat(partitions.get("partition0").getInt("startItem")).isEqualTo(0);
        assertThat(partitions.get("partition0").getInt("endItem")).isEqualTo(1);
        assertThat(partitions.get("partition1").getInt("startItem")).isEqualTo(1);
        assertThat(partitions.get("partition1").getInt("endItem")).isEqualTo(2);
    }

    @Test
    void wrapsIoExceptionFromTheResourceInUncheckedIOException() {
        Resource brokenResource = new ByteArrayResource("irrelevant".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public InputStream getInputStream() throws IOException {
                throw new IOException("boom");
            }
        };

        assertThatThrownBy(() -> new CsvLinePartitioner(brokenResource, 1))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(IOException.class);
    }
}

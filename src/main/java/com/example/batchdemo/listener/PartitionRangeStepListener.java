package com.example.batchdemo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.stereotype.Component;

/**
 * Basic feature: StepExecutionListener - logs the line range each partition worker was
 * assigned. Spring Batch's own "Executing step: [...]" log line only prints the step
 * name, not the startItem/endItem values CsvLinePartitioner put into that partition's
 * ExecutionContext, so without this listener there's no visibility into which lines a
 * given worker thread is actually handling.
 */
@Component
public class PartitionRangeStepListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(PartitionRangeStepListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext context = stepExecution.getExecutionContext();
        if (context.containsKey("startItem")) {
            log.info("Step '{}' starting on partition '{}': lines [{}, {})",
                    stepExecution.getStepName(),
                    context.getString("partitionName", "n/a"),
                    context.getInt("startItem"),
                    context.getInt("endItem"));
        }
    }
}

package com.example.batchdemo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

/**
 * Basic feature: SkipListener - notified whenever an item is skipped during
 * read, process or write, as configured with faultTolerant().skip(...).
 */
@Component
public class CsvStepSkipListener implements SkipListener<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(CsvStepSkipListener.class);

    @Override
    public void onSkipInRead(Throwable t) {
        if (t instanceof FlatFileParseException parseException) {
            log.warn("Skipped line {} while reading ('{}'): {}",
                    parseException.getLineNumber(), parseException.getInput(), parseException.getMessage());
        } else {
            log.warn("Skipped a line while reading: {}", t.getMessage());
        }
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        log.warn("Skipped item '{}' while processing: {}", item, t.getMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        log.warn("Skipped item '{}' while writing: {}", item, t.getMessage());
    }
}

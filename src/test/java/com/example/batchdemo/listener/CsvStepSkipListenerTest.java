package com.example.batchdemo.listener;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for CsvStepSkipListener, exercising all three SkipListener callbacks
 * directly (no Spring context needed) and asserting on the logged message content.
 */
class CsvStepSkipListenerTest {

    private final CsvStepSkipListener listener = new CsvStepSkipListener();
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void attachLogAppender() {
        logAppender = new ListAppender<>();
        logAppender.start();
        ((Logger) LoggerFactory.getLogger(CsvStepSkipListener.class)).addAppender(logAppender);
    }

    @AfterEach
    void detachLogAppender() {
        ((Logger) LoggerFactory.getLogger(CsvStepSkipListener.class)).detachAppender(logAppender);
    }

    @Test
    void onSkipInRead_logsLineNumberAndInputForAFlatFileParseException() {
        listener.onSkipInRead(new FlatFileParseException("bad line", "raw,input", 42));

        assertThat(logAppender.list).hasSize(1);
        assertThat(logAppender.list.get(0).getFormattedMessage())
                .contains("42")
                .contains("raw,input");
    }

    @Test
    void onSkipInRead_logsAGenericMessageForAnyOtherThrowable() {
        listener.onSkipInRead(new IllegalStateException("something else"));

        assertThat(logAppender.list).hasSize(1);
        assertThat(logAppender.list.get(0).getFormattedMessage()).contains("something else");
    }

    @Test
    void onSkipInProcess_logsTheSkippedItem() {
        listener.onSkipInProcess("bad-item", new RuntimeException("processing failed"));

        assertThat(logAppender.list).hasSize(1);
        assertThat(logAppender.list.get(0).getFormattedMessage())
                .contains("bad-item")
                .contains("processing failed");
    }

    @Test
    void onSkipInWrite_logsTheSkippedItem() {
        listener.onSkipInWrite("bad-item", new RuntimeException("write failed"));

        assertThat(logAppender.list).hasSize(1);
        assertThat(logAppender.list.get(0).getFormattedMessage())
                .contains("bad-item")
                .contains("write failed");
    }
}

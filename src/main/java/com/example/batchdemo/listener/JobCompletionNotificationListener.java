package com.example.batchdemo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Basic feature: JobExecutionListener - hooks into beforeJob/afterJob.
 * Useful for summary logging, notifications, cleanup, etc.
 */
@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("=== Job '{}' is starting ===", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("=== Job finished successfully. Verifying results ===");
            jdbcTemplate.query(
                    "SELECT first_name, last_name FROM people",
                    (rs, row) -> rs.getString(1) + " " + rs.getString(2)
            ).forEach(name -> log.info("Found <{}> in the database.", name));
        } else {
            log.warn("=== Job finished with status {} ===", jobExecution.getStatus());
        }
    }
}

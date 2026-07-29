package com.example.batchdemo.config;

import org.springframework.batch.core.configuration.support.JdbcDefaultBatchConfiguration;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot's own batch auto-configuration (as of Spring Boot 4.1 / Spring Batch 6)
 * defaults to {@code ResourcelessJobRepository}: an in-memory, single-execution
 * JobRepository meant for one-shot jobs in their own JVM. It doesn't persist to the
 * database at all (no BATCH_* tables), and it can't do real restarts (it can't tell
 * that a step already completed in a previous execution, since it only ever remembers
 * one JobInstance/JobExecution at a time).
 *
 * Extending {@link JdbcDefaultBatchConfiguration} switches to a real JDBC-backed
 * JobRepository (using the "dataSource"/"transactionManager" beans already provided by
 * spring-boot-starter-jdbc), so job/step execution history is actually persisted in H2
 * and restart semantics (skip already-completed steps) work correctly. The BATCH_*
 * schema itself is created via schema-locations in application.yml.
 */
@Configuration
public class JdbcBatchRepositoryConfig extends JdbcDefaultBatchConfiguration {
}

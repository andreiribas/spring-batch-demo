package com.example.batchdemo.config;

import com.example.batchdemo.domain.Person;
import com.example.batchdemo.job.CsvLinePartitioner;
import com.example.batchdemo.job.PersonItemProcessor;
import com.example.batchdemo.listener.CsvStepSkipListener;
import com.example.batchdemo.listener.JobCompletionNotificationListener;
import com.example.batchdemo.listener.PartitionRangeStepListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Wires together a single Job made of three Steps, to showcase the most
 * common Spring Batch building blocks:
 *
 *   1. helloStep      - a Tasklet step (simple, non chunk-oriented work)
 *   2. importPeoplePartitionedStep - a partitioned chunk-oriented step: the CSV is
 *                          split into line ranges (CsvLinePartitioner) and each
 *                          range is read -> processed -> written concurrently by
 *                          its own worker thread, with fault tolerance (skip)
 *                          turned on and inserts batched via JdbcBatchItemWriter
 *   3. summaryStep    - another Tasklet step that reports on what was written
 *
 * Steps are chained with .next(), so the job only proceeds if the previous
 * step completed successfully.
 */
@Configuration
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    /** Number of concurrent partitions (and worker threads) the CSV import step is split into. */
    private static final int GRID_SIZE = 4;

    /** Number of items committed - and batched into the DB - per chunk, within each partition. */
    private static final int CHUNK_SIZE = 10;

    // ---- Step 1: Tasklet ---------------------------------------------------

    /**
     * Basic feature: restartability driven by job parameters.
     *
     * {@code forceFailure} is late-bound from the job parameters via SpEL, which
     * requires this bean to be step-scoped (resolved fresh per step execution
     * instead of once at context startup). Launch the job with
     * {@code forceFailure=true} to make this step throw, then relaunch with the
     * SAME identifying parameters and {@code forceFailure=false}: Spring Batch
     * recognizes it as a restart of the same (failed) JobInstance, re-runs only
     * this failed step, and lets the job complete.
     */
    @Bean
    @StepScope
    public Tasklet helloTasklet(@Value("#{jobParameters['forceFailure'] ?: 'false'}") String forceFailure) {
        return (contribution, chunkContext) -> {
            if (Boolean.parseBoolean(forceFailure)) {
                throw new IllegalStateException("Simulated failure (forceFailure=true) - restart with forceFailure=false to recover");
            }
            log.info("Hello from a Tasklet step! Kicking off the person import...");
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step helloStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, Tasklet helloTasklet) {
        return new StepBuilder("helloStep", jobRepository)
                .tasklet(helloTasklet, transactionManager)
                .build();
    }

    // ---- Step 2: Partitioned, chunk-oriented (reader / processor / writer) -

    /**
     * Basic feature: Partitioner - divides the CSV into contiguous line ranges,
     * one per partition, so {@code GRID_SIZE} worker threads can each stream and
     * import their own slice of the file concurrently. See {@link CsvLinePartitioner}.
     */
    @Bean
    public Partitioner csvLinePartitioner(@Value("classpath:people.csv") Resource resource) {
        return new CsvLinePartitioner(resource, 1); // skip the CSV header when counting lines
    }

    /**
     * Backs the partition worker threads with virtual threads (Project Loom) instead of a
     * fixed pool of platform threads - pooling doesn't apply to virtual threads, they're
     * meant to be created cheaply per task, so a new one is spawned per partition here.
     */
    @Bean
    public TaskExecutor batchTaskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor("csv-partition-");
        taskExecutor.setVirtualThreads(true);
        return taskExecutor;
    }

    /**
     * Step-scoped so each partition gets its own reader instance, bound (via SpEL) to the
     * {@code startItem}/{@code endItem} line range that {@link CsvLinePartitioner} put into
     * that partition's ExecutionContext. {@code currentItemCount}/{@code maxItemCount} bound
     * the reader to just that range; the file itself is still read line-by-line (streamed),
     * never loaded into memory as a whole.
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Person> personItemReader(
            @Value("classpath:people.csv") Resource resource,
            @Value("#{stepExecutionContext['startItem']}") int startItem,
            @Value("#{stepExecutionContext['endItem']}") int endItem) {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(resource)
                .linesToSkip(1) // skip the CSV header
                .currentItemCount(startItem) // skip ahead to this partition's first data line
                .maxItemCount(endItem) // stop after this partition's last data line
                .delimited()
                .names("firstName", "lastName")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> personItemProcessor() {
        return new PersonItemProcessor();
    }

    /** Batches inserts (JDBC batch update) once per chunk instead of one statement per row. */
    @Bean
    public JdbcBatchItemWriter<Person> personItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .beanMapped()
                .build();
    }

    /** The worker step: one instance of this runs per partition, each on its own thread. */
    @Bean
    public Step importPeopleStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  FlatFileItemReader<Person> personItemReader,
                                  ItemProcessor<Person, Person> personItemProcessor,
                                  ItemWriter<Person> personItemWriter,
                                  CsvStepSkipListener csvStepSkipListener,
                                  PartitionRangeStepListener partitionRangeStepListener) {
        return new StepBuilder("importPeopleStep", jobRepository)
                .<Person, Person>chunk(CHUNK_SIZE) // commit (and batch-write) every CHUNK_SIZE items
                .reader(personItemReader)
                .processor(personItemProcessor)
                .writer(personItemWriter)
                .faultTolerant()
                .skipLimit(2)
                .skip(FlatFileParseException.class) // tolerate a couple of malformed lines
                .listener(csvStepSkipListener)
                .listener(partitionRangeStepListener)
                .build();
    }

    /**
     * Basic feature: step partitioning - fans the worker step above out across
     * {@code GRID_SIZE} threads, one per line range produced by csvLinePartitioner.
     * The job only sees this master step; Spring Batch aggregates all partitions'
     * StepExecutions into a single result for it.
     */
    @Bean
    public Step importPeoplePartitionedStep(JobRepository jobRepository,
                                             Step importPeopleStep,
                                             Partitioner csvLinePartitioner,
                                             TaskExecutor batchTaskExecutor) {
        return new StepBuilder("importPeoplePartitionedStep", jobRepository)
                .partitioner(importPeopleStep.getName(), csvLinePartitioner)
                .step(importPeopleStep)
                .gridSize(GRID_SIZE)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    // ---- Step 3: Tasklet that reports a summary -----------------------------

    @Bean
    public Step summaryStep(JobRepository jobRepository,
                             PlatformTransactionManager transactionManager,
                             JdbcTemplate jdbcTemplate) {
        return new StepBuilder("summaryStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM people", Integer.class);
                    log.info("Summary step: {} people currently stored in the database.", count);
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // ---- The Job: chains the three steps in order ---------------------------

    @Bean
    public Job importPersonJob(JobRepository jobRepository, Step helloStep, Step importPeoplePartitionedStep, Step summaryStep,
                                JobCompletionNotificationListener jobCompletionNotificationListener) {
        return new JobBuilder("importPersonJob", jobRepository)
                .start(helloStep)
                .next(importPeoplePartitionedStep)
                .next(summaryStep)
                .listener(jobCompletionNotificationListener)
                .build();
    }
}

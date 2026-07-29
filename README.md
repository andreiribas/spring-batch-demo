# spring-batch-demo

A minimal Spring Boot + Spring Batch project to learn the core building blocks.
Everything runs against an in-memory H2 database, so you can just run it and
read the logs — no external setup needed.

## Run it

```bash
mvn spring-boot:run
```

The job runs automatically on startup (`spring.batch.job.enabled=true`).
Watch the console: you'll see each step execute, items being processed, one
line being skipped, and a final summary.

## Run the test

```bash
mvn test
```

`ImportPersonJobIntegrationTest` uses `JobLauncherTestUtils` to launch the
whole job in-process and assert on the resulting `JobExecution` and the data
written to the DB.

## What it demonstrates

The job (`importPersonJob`) has three steps, chained with `.next(...)`:

1. **`helloStep`** — a **Tasklet** step. Tasklets are for simple, non
   chunk-oriented work (a single unit of logic executed once), as opposed to
   processing a stream of items.

2. **`importPeopleStep`** — a classic **chunk-oriented step**:
   `reader -> processor -> writer`, committing every 3 items (see
   `.chunk(3, transactionManager)`).
   - **`ItemReader`**: `FlatFileItemReader` reads `people.csv` from the
     classpath and maps each line to a `Person`.
   - **`ItemProcessor`**: `PersonItemProcessor` uppercases the names
     (this is also where you'd validate, enrich, or filter items by
     returning `null`).
   - **`ItemWriter`**: `JdbcBatchItemWriter` batches inserts into the
     `people` H2 table.
   - **Fault tolerance**: `people.csv` has one malformed line (too many
     columns). `.faultTolerant().skip(FlatFileParseException.class)`
     configured with a `skipLimit` lets the step tolerate it instead of
     failing the whole job. A `SkipListener` (`CsvStepSkipListener`) logs
     every skip.

3. **`summaryStep`** — another Tasklet, querying the DB via `JdbcTemplate`
   to report how many rows ended up in `people`.

Other features wired in:

- **`JobExecutionListener`** (`JobCompletionNotificationListener`) — hooks
  into `beforeJob` / `afterJob` to log a summary and verify the data once the
  job completes.
- **Spring Batch metadata tables** (`BATCH_JOB_INSTANCE`,
  `BATCH_STEP_EXECUTION`, etc.) — auto-created on startup via
  `spring.batch.jdbc.initialize-schema=always`. These are what Spring Batch
  uses internally to track job/step executions, restartability, and
  parameters.
- **Spring Batch Test** (`spring-batch-test`) — `@SpringBatchTest` +
  `JobLauncherTestUtils` to launch and assert on the job in a test, without
  needing a running application.

## Where to go next

Ideas for extending this project as you learn more:

- Add a second `Job` and trigger it via a REST endpoint instead of on
  startup (set `spring.batch.job.enabled=false` and use `JobLauncher`
  directly).
- Add **retry** (`.retry(SomeTransientException.class)`) alongside the
  existing skip logic, to distinguish "retry a flaky operation" from "give
  up on this one bad item".
- Add **conditional flow** between steps (`.on("FAILED").to(...)`) instead
  of always calling `.next(...)`.
- Split `importPeopleStep` across threads with a `TaskExecutor` or
  partitioning, to see parallel chunk processing.
- Make the job restartable on failure by removing `allowStartIfComplete`
  defaults and intentionally throwing partway through a chunk.

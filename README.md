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
   - Its tasklet bean (`helloTasklet`) is **`@StepScope`** and late-binds a
     `forceFailure` job parameter via SpEL
     (`#{jobParameters['forceFailure'] ?: 'false'}`). When `forceFailure=true`
     it throws, letting you demo a job failing and then being **restarted**
     — see [Restarting a failed job via job parameters](#restarting-a-failed-job-via-job-parameters)
     below.

2. **`importPeopleStep`** — a classic **chunk-oriented step**:
   `reader -> processor -> writer`, committing every 3 items (see
   `.chunk(3)`).
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
     every skip, including the line number and raw offending text (both
     available on the `FlatFileParseException` itself via `getLineNumber()`
     / `getInput()`).

3. **`summaryStep`** — another Tasklet, querying the DB via `JdbcTemplate`
   to report how many rows ended up in `people`.

Other features wired in:

- **`JobExecutionListener`** (`JobCompletionNotificationListener`) — hooks
  into `beforeJob` / `afterJob` to log a summary and verify the data once the
  job completes. Registered on the job explicitly via `.listener(...)` in
  `BatchConfig` — being a `@Component` alone does **not** attach a listener
  to a job.
- **Spring Batch metadata tables** (`BATCH_JOB_INSTANCE`,
  `BATCH_JOB_EXECUTION`, `BATCH_STEP_EXECUTION`, etc.) — these are what
  Spring Batch uses internally to track job/step executions, restartability,
  and parameters. See the callout below on how these get created in this
  project — it's not what you'd expect from older Spring Boot versions.
- **Spring Batch Test** (`spring-batch-test`) — `@SpringBatchTest` +
  `JobLauncherTestUtils` to launch and assert on the job in a test, without
  needing a running application.

### Gotcha: Spring Boot 4.1 / Spring Batch 6 no longer auto-configures a JDBC job repository

This project is pinned to `spring-boot-starter-parent:4.1.0` (Spring Batch 6).
In older Spring Boot versions, just having `spring-boot-starter-jdbc` + an SQL
database on the classpath, plus `spring.batch.jdbc.initialize-schema=always`,
was enough for Spring Boot to create the `BATCH_*` tables and back the
`JobRepository` with your database. **That property doesn't exist anymore** —
`BatchProperties` in `spring-boot-batch:4.1.0` only has `spring.batch.job.name`
and `spring.batch.job.enabled`.

Instead, Spring Boot's default batch auto-configuration now wires up
`ResourcelessJobRepository`: an **in-memory, single-execution** `JobRepository`
explicitly intended for one-shot jobs that run and die in their own JVM. It
doesn't persist anything to the database (no `BATCH_*` tables at all), and it
can't really restart a job either — it only ever remembers one `JobInstance`
and one `JobExecution` at a time, so it can't tell that a step already
completed in a previous execution.

To get real, persisted restart behavior, this project:

- Defines `JdbcBatchRepositoryConfig` (`config/JdbcBatchRepositoryConfig.java`),
  which extends Spring Batch's `JdbcDefaultBatchConfiguration` to wire a real
  JDBC-backed `JobRepository` (using the existing `dataSource` /
  `transactionManager` beans from `spring-boot-starter-jdbc`).
- Adds Spring Batch's own bundled schema script as an extra schema location in
  `application.yml`:
  ```yaml
  spring:
    sql:
      init:
        schema-locations:
          - classpath:schema.sql
          - classpath:org/springframework/batch/core/schema-h2.sql
  ```
  (that second path is a resource packaged inside the `spring-batch-core` jar
  itself — no need to copy it into this project).

With that in place, `BATCH_JOB_EXECUTION` etc. really exist in H2, and a
restarted `JobExecution` gets a new execution row attached to the *same*
`JobInstance`, with steps that already completed correctly skipped.

## Restarting a failed job via job parameters

The `JobTriggerController` (`web/JobTriggerController.java`, backed by
`spring-boot-starter-web`) exposes the job on demand, so you can trigger it
twice against the *same running app* — no need to restart the JVM — and watch
a failed job get restarted successfully:

- `POST /jobs/import?forceFailure=true|false` — launches `importPersonJob`
  with two job parameters:
  - `trigger=rest-api` — **identifying**, kept constant on every call, so
    every call targets the *same* `JobInstance`.
  - `forceFailure=<value>` — **non-identifying**, free to change between
    calls without Spring Batch treating it as a different `JobInstance`.

  Whether a parameter is identifying is controlled by the third argument to
  `JobParametersBuilder.addString(name, value, identifying)` — the 2-arg
  overload defaults to `identifying=true`. In this project that's spelled out
  with named constants (`IDENTIFYING` / `NON_IDENTIFYING`) instead of a bare
  `true`/`false` literal, precisely because that boolean is easy to miss.
- `GET /jobs/debug` — a diagnostic endpoint that dumps the raw
  `BATCH_JOB_EXECUTION` rows (execution id, instance id, status), so you can
  see `JobInstance`/`JobExecution` identity directly against the database
  instead of just trusting the logs.

Demo sequence:

1. `POST /jobs/import?forceFailure=true` → `helloTasklet` throws →
   job **FAILED**.
2. `POST /jobs/import?forceFailure=false` (same identifying `trigger`
   parameter) → Spring Batch recognizes this as a **restart of the same
   JobInstance**: `helloStep` (previously failed) re-runs and succeeds, and
   the job proceeds through `importPeopleStep` / `summaryStep` for the first
   time → job **COMPLETED**.
3. Calling it a third time with the same parameters throws
   `JobInstanceAlreadyCompleteException` (surfaced as HTTP 409) — that
   `JobInstance` already completed successfully, so Spring Batch refuses to
   run it again unless you change the parameters.

Run `./restart-demo.sh` (with the app already running via
`mvn spring-boot:run`) to walk through this automatically, checking
`/jobs/debug` before and after each call so you can see the instance/execution
ids change at each step:

1. `/jobs/debug` — baseline, before anything runs.
2. `POST /jobs/import?forceFailure=true` — job **FAILS**.
3. `/jobs/debug` — a new `JobInstance`/`JobExecution` appears with status
   `FAILED`.
4. `POST /jobs/import?forceFailure=false` — restarts the same `JobInstance`;
   job **COMPLETES**.
5. `/jobs/debug` — same instance id as step 3, but a *new* execution id with
   status `COMPLETED`.
6. `POST /jobs/import?forceFailure=false` again — same parameters as an
   already-completed `JobInstance`, so this call **fails** with HTTP 409
   (`JobInstanceAlreadyCompleteException`).
7. `/jobs/debug` — unchanged from step 5, confirming the rejected rerun in
   step 6 created nothing new.

Override the target host with `BASE_URL=http://localhost:8080 ./restart-demo.sh`.

## Where to go next

Ideas for extending this project as you learn more:

- Add **retry** (`.retry(SomeTransientException.class)`) alongside the
  existing skip logic, to distinguish "retry a flaky operation" from "give
  up on this one bad item".
- Add **conditional flow** between steps (`.on("FAILED").to(...)`) instead
  of always calling `.next(...)`.
- Split `importPeopleStep` across threads with a `TaskExecutor` or
  partitioning, to see parallel chunk processing.
- Extend the restart demo to fail *inside* `importPeopleStep` (e.g. after a
  couple of chunks committed) instead of in `helloStep`, to see Spring Batch
  resume a chunk-oriented step from where it left off, rather than skipping a
  step wholesale.

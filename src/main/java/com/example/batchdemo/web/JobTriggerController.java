package com.example.batchdemo.web;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Basic feature: restarting a failed job via job parameters.
 *
 * {@code trigger=rest-api} is an identifying parameter: kept constant across
 * calls so every call targets the SAME JobInstance. {@code forceFailure} is a
 * non-identifying parameter: it can change value between calls without
 * Spring Batch treating it as a different JobInstance.
 *
 * Demo:
 *   curl -X POST 'localhost:8080/jobs/import?forceFailure=true'   -> job FAILS in helloStep
 *   curl -X POST 'localhost:8080/jobs/import?forceFailure=false'  -> restarts the same
 *     JobInstance: helloStep (previously failed) re-runs and succeeds, the job proceeds
 *     through importPeopleStep/summaryStep for the first time, and COMPLETES.
 *
 * A third call with the same parameters would throw JobInstanceAlreadyCompleteException,
 * since that JobInstance already completed successfully.
 */
@RestController
public class JobTriggerController {

    private static final boolean IDENTIFYING = true;
    private static final boolean NON_IDENTIFYING = false;

    private final JobOperator jobOperator;
    private final Job importPersonJob;
    private final JdbcTemplate jdbcTemplate;

    public JobTriggerController(JobOperator jobOperator, Job importPersonJob, JdbcTemplate jdbcTemplate) {
        this.jobOperator = jobOperator;
        this.importPersonJob = importPersonJob;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Diagnostic: raw view of the persisted BATCH_JOB_EXECUTION rows, useful for
     * confirming JobInstance/JobExecution identity across calls to /jobs/import.
     */
    @GetMapping("/jobs/debug")
    public List<String> debug() {
        return jdbcTemplate.query(
                "SELECT JOB_EXECUTION_ID, JOB_INSTANCE_ID, STATUS FROM BATCH_JOB_EXECUTION ORDER BY JOB_EXECUTION_ID",
                (rs, rowNum) -> "execId=%s instanceId=%s status=%s".formatted(
                        rs.getString(1), rs.getString(2), rs.getString(3)));
    }

    @PostMapping("/jobs/import")
    public ResponseEntity<String> triggerImport(@RequestParam(defaultValue = "false") boolean forceFailure) {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("trigger", "rest-api", IDENTIFYING) // same value every call -> same JobInstance
                .addString("forceFailure", String.valueOf(forceFailure), NON_IDENTIFYING) // free to vary between calls
                .toJobParameters();

        try {
            JobExecution jobExecution = jobOperator.start(importPersonJob, jobParameters);
            return ResponseEntity.ok(
                    "JobInstance id=%d / JobExecution id=%d status=%s exitStatus=%s".formatted(
                            jobExecution.getJobInstance().getId(), jobExecution.getId(), jobExecution.getStatus(),
                            jobExecution.getExitStatus()));
        } catch (JobInstanceAlreadyCompleteException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("This JobInstance already completed successfully: " + e.getMessage());
        } catch (JobExecutionAlreadyRunningException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("This job is already running: " + e.getMessage());
        } catch (JobRestartException | InvalidJobParametersException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}

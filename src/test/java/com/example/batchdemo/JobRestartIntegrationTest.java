package com.example.batchdemo;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Basic feature: restarting a failed job via job parameters (the mechanics
 * behind JobTriggerController / restart-demo.sh), driving JobOperator
 * directly instead of going through HTTP.
 *
 * {@code @DirtiesContext} isolates this class's JobRepository/H2 state from
 * other test classes, since the jobs run here actually write to the `people`
 * table and would otherwise affect row-count assertions elsewhere.
 */
@ExtendWith(SpringExtension.class)
@SpringBatchTest
@SpringBootTest(properties = "spring.batch.job.enabled=false")
@DirtiesContext
class JobRestartIntegrationTest {

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job importPersonJob;

    private static JobParameters parameters(String trigger, boolean forceFailure) {
        return new JobParametersBuilder()
                .addString("trigger", trigger)
                .addString("forceFailure", String.valueOf(forceFailure), false)
                .toJobParameters();
    }

    @Test
    void failedJobCanBeRestartedWithSameIdentifyingParameters() throws Exception {
        JobExecution failedExecution = jobOperator.start(importPersonJob, parameters("restart-test", true));
        assertThat(failedExecution.getStatus()).isEqualTo(BatchStatus.FAILED);

        JobExecution restartedExecution = jobOperator.start(importPersonJob, parameters("restart-test", false));

        assertThat(restartedExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(restartedExecution.getJobInstance().getId())
                .isEqualTo(failedExecution.getJobInstance().getId());
        assertThat(restartedExecution.getId()).isNotEqualTo(failedExecution.getId());
    }

    @Test
    void rerunningACompletedJobInstanceIsRejected() throws Exception {
        JobParameters completedRunParameters = parameters("already-complete-test", false);

        JobExecution firstExecution = jobOperator.start(importPersonJob, completedRunParameters);
        assertThat(firstExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThatThrownBy(() -> jobOperator.start(importPersonJob, completedRunParameters))
                .isInstanceOf(JobInstanceAlreadyCompleteException.class);
    }
}

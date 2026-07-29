package com.example.batchdemo;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic feature: Spring Batch Test support - JobLauncherTestUtils lets you
 * launch the whole Job (or a single Step) in a test and assert on the
 * resulting JobExecution / final data state.
 */
@ExtendWith(SpringExtension.class)
@SpringBatchTest
@SpringBootTest(properties = "spring.batch.job.enabled=false") // don't auto-run on context startup; we launch it explicitly below
class ImportPersonJobIntegrationTest {

    @Autowired
    private JobOperatorTestUtils jobOperatorTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void wholeJobCompletesAndSkipsTheMalformedLine() throws Exception {
        JobExecution jobExecution = jobOperatorTestUtils.startJob();

        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM people", Integer.class);
        // 200 valid rows in people.csv, 1 malformed row is skipped
        assertThat(count).isEqualTo(200);
    }
}

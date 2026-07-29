package com.example.batchdemo.web;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.sql.ResultSet;
import java.util.List;

import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Basic feature: MockMvc + @MockitoBean - exercises JobTriggerController's HTTP
 * behavior (success body, and the exception-to-status-code mapping) without a
 * real JobRepository, by mocking JobOperator/Job/JdbcTemplate.
 */
@WebMvcTest(JobTriggerController.class)
class JobTriggerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JobOperator jobOperator;

    @MockitoBean
    private Job importPersonJob;

    @MockitoBean
    private JdbcTemplate jdbcTemplate;

    @Test
    void triggerImport_returnsExecutionDetails_whenJobStartsSuccessfully() throws Exception {
        JobInstance jobInstance = new JobInstance(2L, "importPersonJob");
        JobExecution jobExecution = new JobExecution(3L, jobInstance, new JobParameters());
        jobExecution.setStatus(BatchStatus.COMPLETED);
        jobExecution.setExitStatus(ExitStatus.COMPLETED);

        when(jobOperator.start(eq(importPersonJob), any(JobParameters.class))).thenReturn(jobExecution);

        mockMvc.perform(post("/jobs/import").param("forceFailure", "false"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("JobInstance id=2")))
                .andExpect(content().string(containsString("JobExecution id=3")))
                .andExpect(content().string(containsString("COMPLETED")));
    }

    @Test
    void triggerImport_returns409_whenJobInstanceAlreadyComplete() throws Exception {
        when(jobOperator.start(eq(importPersonJob), any(JobParameters.class)))
                .thenThrow(new JobInstanceAlreadyCompleteException("already complete"));

        mockMvc.perform(post("/jobs/import").param("forceFailure", "false"))
                .andExpect(status().isConflict())
                .andExpect(content().string(containsString("already completed successfully")));
    }

    @Test
    void triggerImport_returns409_whenJobExecutionAlreadyRunning() throws Exception {
        when(jobOperator.start(eq(importPersonJob), any(JobParameters.class)))
                .thenThrow(new JobExecutionAlreadyRunningException("already running"));

        mockMvc.perform(post("/jobs/import").param("forceFailure", "true"))
                .andExpect(status().isConflict())
                .andExpect(content().string(containsString("already running")));
    }

    @Test
    void triggerImport_returns400_whenJobRestartIsNotAllowed() throws Exception {
        when(jobOperator.start(eq(importPersonJob), any(JobParameters.class)))
                .thenThrow(new JobRestartException("not restartable"));

        mockMvc.perform(post("/jobs/import").param("forceFailure", "true"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void triggerImport_returns400_whenJobParametersAreInvalid() throws Exception {
        when(jobOperator.start(eq(importPersonJob), any(JobParameters.class)))
                .thenThrow(new InvalidJobParametersException("invalid parameters"));

        mockMvc.perform(post("/jobs/import").param("forceFailure", "true"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @SuppressWarnings("unchecked")
    void debug_returnsRowsQueriedFromBatchJobExecutionTable() throws Exception {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of("execId=1 instanceId=1 status=COMPLETED"));

        mockMvc.perform(get("/jobs/debug"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("execId=1 instanceId=1 status=COMPLETED")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void debug_rowMapperFormatsResultSetColumnsAsExpected() throws Exception {
        ArgumentCaptor<RowMapper<String>> rowMapperCaptor = ArgumentCaptor.forClass(RowMapper.class);
        when(jdbcTemplate.query(anyString(), rowMapperCaptor.capture())).thenReturn(List.of());

        mockMvc.perform(get("/jobs/debug")).andExpect(status().isOk());

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString(1)).thenReturn("10");
        when(resultSet.getString(2)).thenReturn("20");
        when(resultSet.getString(3)).thenReturn("COMPLETED");

        String mapped = rowMapperCaptor.getValue().mapRow(resultSet, 0);

        assertThat(mapped).isEqualTo("execId=10 instanceId=20 status=COMPLETED");
    }
}

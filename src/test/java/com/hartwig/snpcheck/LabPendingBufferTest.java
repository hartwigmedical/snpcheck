package com.hartwig.snpcheck;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hartwig.events.pipeline.Analysis;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.PipelineComplete;

import org.junit.Test;

public class LabPendingBufferTest {

    @Test
    public void schedulesAnotherRunAfterConfiguredDelay() throws Exception {
        SnpCheck snpCheck = mock(SnpCheck.class);
        LabPendingBuffer victim = new LabPendingBuffer(snpCheck, Executors.newSingleThreadScheduledExecutor(), TimeUnit.MILLISECONDS, 1);
        PipelineComplete event = PipelineComplete.builder()
                .pipeline(Pipeline.builder()
                        .bucket("bucket")
                        .runId(1L)
                        .context(Pipeline.Context.DIAGNOSTIC)
                        .sample("sample")
                        .addAnalyses(Analysis.builder().molecule(Analysis.Molecule.DNA).type(Analysis.Type.SOMATIC).build())
                        .version("5.23")
                        .setId(1L)
                        .build())
                .build();
        victim.add(event);
        Thread.sleep(500);
        verify(snpCheck, times(1)).handle(event);
    }
}
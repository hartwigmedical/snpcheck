package com.hartwig.snpcheck;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hartwig.events.Analysis;
import com.hartwig.events.PipelineStaged;

import org.junit.Test;

public class LabPendingBufferTest {

    @Test
    public void schedulesAnotherRunAfterConfiguredDelay() throws Exception {
        SnpCheck snpCheck = mock(SnpCheck.class);
        LabPendingBuffer victim = new LabPendingBuffer(snpCheck, Executors.newSingleThreadScheduledExecutor(), TimeUnit.MILLISECONDS, 1);
        PipelineStaged event = PipelineStaged.builder()
                .sample("sample")
                .analysisContext(Analysis.Context.DIAGNOSTIC)
                .analysisMolecule(Analysis.Molecule.DNA)
                .analysisType(Analysis.Type.TERTIARY)
                .version("5.23")
                .setId(1L)
                .build();
        victim.add(event);
        Thread.sleep(500);
        verify(snpCheck, times(1)).handle(event);
    }
}
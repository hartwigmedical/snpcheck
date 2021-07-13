package com.hartwig.snpcheck;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hartwig.events.PipelineStaged;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LabPendingBuffer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LabPendingBuffer.class);

    private final SnpCheck snpCheck;
    private final ScheduledExecutorService scheduler;
    private final TimeUnit delayUnit;
    private final int delay;

    public LabPendingBuffer(final SnpCheck snpCheck, final ScheduledExecutorService scheduler, final TimeUnit delayUnit, final int delay) {
        this.snpCheck = snpCheck;
        this.scheduler = scheduler;
        this.delayUnit = delayUnit;
        this.delay = delay;
    }

    public void add(final PipelineStaged buffered) {
        LOGGER.info("Scheduling sample [{}] to be reprocessed in 1 hour", buffered.sample());
        scheduler.schedule(() -> {
            LOGGER.info("Reprocessing sample [{}] as lab VCF was not available on last attempt", buffered.sample());
            snpCheck.handle(buffered);
        }, delay, delayUnit);
    }
}
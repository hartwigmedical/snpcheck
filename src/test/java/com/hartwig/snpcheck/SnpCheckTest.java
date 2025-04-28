package com.hartwig.snpcheck;

import static com.hartwig.events.pipeline.Pipeline.Context.RESEARCH;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.RunFailure.TypeEnum;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.events.EventPublisher;
import com.hartwig.events.aqua.SnpCheckCompletedEvent;
import com.hartwig.events.aqua.model.AquaEvent;
import com.hartwig.events.aqua.model.AquaEventType;
import com.hartwig.events.local.LocalEventBuilder;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.Pipeline.Context;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pipeline.PipelineValidated;
import com.hartwig.events.turquoise.TurquoiseEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

public class SnpCheckTest {

    private static final long SET_ID = 2L;
    public static final long RUN_ID = 1L;
    private static final String BARCODE = "barcode";
    private static final Sample REF_SAMPLE = new Sample().barcode(BARCODE).name("sampler");
    private static final Sample TUMOR_SAMPLE = new Sample().barcode(BARCODE).name("samplet");

    private Run run;
    private RunApi runApi;
    private SampleApi sampleApi;
    private EventPublisher<TurquoiseEvent> turquoiseTopicPublisher;
    private EventPublisher<PipelineValidated> validatedTopicPublisher;
    private SnpCheck victim;
    private LocalEventBuilder eventBuilder;
    private EventPublisher<AquaEvent> aquaTopicPublisher;

    @BeforeEach
    public void setUp() {
        run = run(Ini.SOMATIC_INI);
        runApi = mock(RunApi.class);
        sampleApi = mock(SampleApi.class);
        eventBuilder = new LocalEventBuilder();
        turquoiseTopicPublisher = eventBuilder.newPublisher("turquoise", new TurquoiseEvent.EventDescriptor());
        aquaTopicPublisher = eventBuilder.newPublisher("aqua", new AquaEvent.EventDescriptor());
        validatedTopicPublisher = eventBuilder.newPublisher("validated", new PipelineValidated.EventDescriptor());
        when(runApi.get(RUN_ID)).thenReturn(run);
        when(sampleApi.callList(null, null, null, SET_ID, SampleType.REF, null, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.callList(null, null, null, SET_ID, SampleType.TUMOR, null, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        victim = new SnpCheck(runApi, sampleApi, turquoiseTopicPublisher, aquaTopicPublisher, validatedTopicPublisher, false);
    }

    private Run run(final Ini somaticIni) {
        return new Run().bucket("bucket")
                .id(RUN_ID)
                .set(new RunSet().name("set").refSample("ref").id(SET_ID))
                .ini(somaticIni.getValue())
                .status(Status.FINISHED)
                .endTime("2024-01-02T00:00:00Z");
    }

    @Test
    public void processesDiagnosticPipelines() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void processesServicesPipelines() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Context.SERVICES));
    }

    @Test
    public void filtersRunsThatAreNeitherSomaticNorSingleIniRuns() {
        when(runApi.get(run.getId())).thenReturn(new Run().ini(Ini.RERUN_INI.getValue()).status(Status.FINISHED));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void filtersNonFinishedSomaticRuns() {
        when(runApi.get(run.getId())).thenReturn(run.status(Status.PENDING));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void filtersNonFinishedSingleRuns() {
        when(runApi.get(run.getId())).thenReturn(run.status(Status.PENDING).ini(Ini.SINGLESAMPLE_INI.getValue()));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void eventPublishedButRunStaysFailedAfterHealthcheckFailure() {
        var event = stagedEvent(Context.DIAGNOSTIC);
        run = run.status(Status.FAILED).failure(new RunFailure().type(TypeEnum.QCFAILURE).source("HealthCheck"));

        // act
        victim.handle(event);

        verify(runApi, never()).update(any(), any());
        var validatedEvents = eventBuilder.getQueueBuffer(new PipelineValidated.EventDescriptor());
        assertThat(validatedEvents).hasSize(1);
        var validated = validatedEvents.get(0);
        assertWrappedOriginalEvent(validated, Context.DIAGNOSTIC);
    }

    @Test
    public void eventPublishedAndRunIsValidatedAfterSnpCheckFailure() {
        var event = stagedEvent(Context.DIAGNOSTIC);
        run = run.status(Status.FAILED).failure(new RunFailure().type(TypeEnum.QCFAILURE).source("SnpCheck"));

        // act
        victim.handle(event);

        verify(runApi).update(run.getId(), new UpdateRun().status(Status.VALIDATED).failure(null));
        var validatedEvents = eventBuilder.getQueueBuffer(new PipelineValidated.EventDescriptor());
        assertThat(validatedEvents).hasSize(1);
        var validated = validatedEvents.get(0);
        assertWrappedOriginalEvent(validated, Context.DIAGNOSTIC);
    }

    @Test
    public void finishedSomaticRunNoRefSampleMarksRunTechnicalFail() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.callList(null, null, null, SET_ID, SampleType.REF, null, null)).thenReturn(emptyList());

        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        assertTechnicalFailure();
    }

    @Test
    public void finishedSomaticDiagnosticRunPass() {
        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticDiagnosticWithSampleInBarcodeRunPass() {
        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticServicesRunPass() {
        // act
        victim.handle(stagedEvent(Context.SERVICES));

        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSingleSampleRunPass() {
        Run singleSampleRun = run(Ini.SINGLESAMPLE_INI);
        when(runApi.get(run.getId())).thenReturn(singleSampleRun);
        when(sampleApi.callList(null, null, null, SET_ID, SampleType.REF, null, null)).thenReturn(singletonList(REF_SAMPLE));

        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void publishesTurquoiseEventOnCompletion() {
        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        var turquoiseEvents = eventBuilder.getQueueBuffer(new TurquoiseEvent.EventDescriptor());
        assertThat(turquoiseEvents).hasSize(1);
        var event = turquoiseEvents.get(0);
        assertThat(event.type()).isEqualTo("snpcheck.completed");
        assertThat(event.subjects().get(0).name()).isEqualTo("samplet");
        assertThat(event.subjects().get(0).type()).isEqualTo("sample");
    }

    @Test
    public void publishesAquaEventOnDiagnosticCompletion() {
        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        var aquaEvents = eventBuilder.getQueueBuffer(new AquaEvent.EventDescriptor());
        assertThat(aquaEvents).hasSize(1);
        var event = (SnpCheckCompletedEvent) aquaEvents.get(0);
        assertThat(event.type()).isEqualTo(AquaEventType.SNP_CHECK_COMPLETED);
        assertThat(event.barcode()).isEqualTo(BARCODE);
        assertThat(event.snpCheckResult()).isEqualTo("PASS");
        assertThat(event.ini()).isEqualTo(Ini.SOMATIC_INI.getValue());
        assertThat(event.context()).isEqualTo(Context.DIAGNOSTIC);
    }

    @Test
    public void publishesPipelineValidatedEventOnCompletion() {
        // act
        victim.handle(stagedEvent(Context.DIAGNOSTIC));

        var validatedEvents = eventBuilder.getQueueBuffer(new PipelineValidated.EventDescriptor());
        assertThat(validatedEvents).hasSize(1);
        var validated = validatedEvents.get(0);
        assertWrappedOriginalEvent(validated, Context.DIAGNOSTIC);
    }

    @Test
    public void passesThruWhenFlagSet() {
        victim = new SnpCheck(runApi, sampleApi, turquoiseTopicPublisher, aquaTopicPublisher, validatedTopicPublisher, true);
        when(runApi.get(run.getId())).thenReturn(run.ini(Ini.RERUN_INI.getValue()));

        // act
        victim.handle(stagedEvent(RESEARCH));

        var validatedEvents = eventBuilder.getQueueBuffer(new PipelineValidated.EventDescriptor());
        assertThat(validatedEvents).hasSize(1);
        var validated = validatedEvents.get(0);
        assertWrappedOriginalEvent(validated, RESEARCH);
        assertValidatedInApi();
    }

    @ParameterizedTest
    @EnumSource(value = Pipeline.Context.class,
                names = { "RESEARCH", "RESEARCH2" })
    public void passesThruInResearchContexts(Pipeline.Context context) {
        // act
        victim.handle(stagedEvent(context));

        var validatedEvents = eventBuilder.getQueueBuffer(new PipelineValidated.EventDescriptor());
        assertThat(validatedEvents).hasSize(1);
        var validated = validatedEvents.get(0);
        assertWrappedOriginalEvent(validated, context);
        assertValidatedInApi();
    }

    private void assertValidatedInApi() {
        ArgumentCaptor<UpdateRun> updateRunArgumentCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        verify(runApi).update(eq(RUN_ID), updateRunArgumentCaptor.capture());
        assertThat(updateRunArgumentCaptor.getValue().getStatus()).isEqualTo(Status.VALIDATED);
    }

    private void assertWrappedOriginalEvent(PipelineValidated event, Context context) {
        Pipeline pipeline = event.pipeline();
        assertThat(pipeline.context()).isEqualTo(context);
        assertThat(pipeline.sample()).isEqualTo("samplet");
        assertThat(pipeline.version()).isEqualTo("version");
        assertThat(pipeline.runId()).isEqualTo(1);
        assertThat(pipeline.setId()).isEqualTo(2);
    }

    private void setupSnpcheckPassAndVerifyApiAndEventUpdates(PipelineComplete event) {
        victim.handle(event);
        verify(runApi).update(eq(RUN_ID), any(UpdateRun.class));
    }

    private void handleAndVerifyNoApiOrEventUpdates(PipelineComplete event) {
        victim.handle(event);
        verify(runApi, never()).update(any(), any());
    }

    private void assertTechnicalFailure() {
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.FAILED);
        assertThat(update.getFailure()).isEqualTo(new RunFailure().source("SnpCheck").type(RunFailure.TypeEnum.TECHNICALFAILURE));
    }

    private UpdateRun captureUpdate() {
        ArgumentCaptor<UpdateRun> updateRunArgumentCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        verify(runApi).update(eq(run.getId()), updateRunArgumentCaptor.capture());
        return updateRunArgumentCaptor.getValue();
    }

    private PipelineComplete stagedEvent(final Pipeline.Context context) {
        return PipelineComplete.builder()
                .pipeline(Pipeline.builder()
                        .bucket("bucket")
                        .sample(TUMOR_SAMPLE.getName())
                        .version("version")
                        .context(context)
                        .setId(SET_ID)
                        .runId(RUN_ID)
                        .build())
                .build();
    }
}
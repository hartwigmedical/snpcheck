package com.hartwig.snpcheck;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.model.*;
import com.hartwig.api.model.RunFailure.TypeEnum;
import com.hartwig.events.*;
import com.hartwig.events.Analysis.Molecule;
import com.hartwig.events.Analysis.Type;
import com.hartwig.events.Pipeline.Context;
import com.hartwig.snpcheck.VcfComparison.Result;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SnpCheckTest {

    private static final long SET_ID = 2L;
    public static final long RUN_ID = 1L;
    private static final String BARCODE = "barcode";
    private static final Sample REF_SAMPLE = new Sample().barcode(BARCODE).name("sampler");
    private static final Sample TUMOR_SAMPLE = new Sample().barcode(BARCODE).name("samplet");

    private Run run;
    private RunApi runApi;
    private SampleApi sampleApi;
    private Storage pipelineStorage;
    private String snpcheckBucket;
    private VcfComparison vcfComparison;
    private Publisher turquoiseTopicPublisher;
    private Publisher validatedTopicPublisher;
    private SnpCheck victim;
    private AnalysisOutputBlob outputBlob;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new Jdk8Module());
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        run = run(Ini.SOMATIC_INI);
        runApi = mock(RunApi.class);
        sampleApi = mock(SampleApi.class);
        pipelineStorage = mock(Storage.class);
        snpcheckBucket = "bucket";
        vcfComparison = mock(VcfComparison.class);
        turquoiseTopicPublisher = mock(Publisher.class);
        validatedTopicPublisher = mock(Publisher.class);
        when(turquoiseTopicPublisher.publish(any())).thenReturn(mock(ApiFuture.class));
        when(validatedTopicPublisher.publish(any())).thenReturn(mock(ApiFuture.class));
        when(runApi.get(RUN_ID)).thenReturn(run);
        outputBlob = AnalysisOutputBlob.builder()
                .barcode("bc")
                .bucket("bucket")
                .root("root")
                .filename("filename")
                .filesize(11)
                .hash("hash")
                .build();
        victim = new SnpCheck(runApi,
                sampleApi,
                pipelineStorage,
                snpcheckBucket,
                vcfComparison,
                turquoiseTopicPublisher,
                validatedTopicPublisher,
                OBJECT_MAPPER, false, false);
    }

    private Run run(final Ini somaticIni) {
        return new Run().bucket("bucket")
                .id(RUN_ID)
                .set(new RunSet().name("set").refSample("ref").id(SET_ID))
                .ini(somaticIni.getValue())
                .status(Status.FINISHED);
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
    public void noEventPublishedOnSnpCheckFailure() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void eventPublishedButRunStaysFailedOnSnpCheckSuccessAfterHealthcheckFailure() {
        setupHealthcheckPassAndVerifyNoApiUpdateButEvent(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void finishedSomaticRunNoRefSampleMarksRunTechnicalFail() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(emptyList());
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        assertTechnicalFailure();
    }

    @Test
    public void finishedSomaticRunNoValidationVcfDoesNothing() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Context.DIAGNOSTIC));
    }

    @Test
    public void finishedSomaticRunNoRefVcfMarksRunTechnicalFail() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        Page<Blob> page = mockPage();
        Blob validationVcf = mock(Blob.class);
        when(validationVcf.getName()).thenReturn(BARCODE + ".vcf");
        when(page.iterateAll()).thenReturn(singletonList(validationVcf));
        when(pipelineStorage.list(snpcheckBucket, Storage.BlobListOption.prefix(SnpCheck.SNPCHECK_VCFS))).thenReturn(page);
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        assertTechnicalFailure();
    }

    @Test
    public void finishedSomaticDiagnosticRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticDiagnosticWithSampleInBarcodeRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE + "_sampler");
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticServicesRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Context.SERVICES));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticRunComparedToValidationVcfFail() {
        fullSnpcheckWithResult(VcfComparison.Result.FAIL, BARCODE);
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.FAILED);
        assertThat(update.getFailure()).isEqualTo(new RunFailure().source("SnpCheck").type(RunFailure.TypeEnum.QCFAILURE));
    }

    @Test
    public void finishedSingleSampleRunComparedToValidationVcfPass() {
        Run singleSampleRun = run(Ini.SINGLESAMPLE_INI);
        when(runApi.get(run.getId())).thenReturn(singleSampleRun);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        setupValidationVcfs(VcfComparison.Result.PASS, singleSampleRun, BARCODE);
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void publishesTurquoiseEventOnCompletion() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(turquoiseTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        Map<String, Object> event = readEvent(pubsubMessageArgumentCaptor, Map.class);
        assertThat(event.get("type")).isEqualTo("snpcheck.completed");
        List<Map<String, Object>> subjects = (List<Map<String, Object>>) event.get("subjects");
        assertThat(subjects.get(0).get("name")).isEqualTo("samplet");
        assertThat(subjects.get(0).get("type")).isEqualTo("sample");
    }

    @Test
    public void publishesPipelineValidatedEventOnCompletion() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Context.DIAGNOSTIC));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        PipelineValidated validated = readEvent(pubsubMessageArgumentCaptor, PipelineValidated.class);
        assertWrappedOriginalEvent(validated, Context.DIAGNOSTIC);
    }

    @Test
    public void validatesResearchRunsWithDiagnosticSnpcheck() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(runApi.list(null, Ini.SOMATIC_INI, SET_ID, null, null, null, null, null))
                .thenReturn(List.of(new Run().status(Status.VALIDATED).context("RESEARCH"), new Run().status(Status.VALIDATED).context("DIAGNOSTIC")));
        victim.handle(stagedEvent(Context.RESEARCH));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        PipelineValidated validated = readEvent(pubsubMessageArgumentCaptor, PipelineValidated.class);
        assertWrappedOriginalEvent(validated, Context.RESEARCH);
        assertValidatedInApi();
    }

    @Test
    public void validatesResearchRunsWithServicesSnpcheck() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(runApi.list(null, Ini.SOMATIC_INI, SET_ID, null, null, null, null, null))
                .thenReturn(List.of(new Run().status(Status.VALIDATED).context("RESEARCH"), new Run().status(Status.VALIDATED).context("SERVICES")));
        victim.handle(stagedEvent(Context.RESEARCH));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        PipelineValidated validated = readEvent(pubsubMessageArgumentCaptor, PipelineValidated.class);
        assertWrappedOriginalEvent(validated, Context.RESEARCH);
        assertValidatedInApi();
    }

    @Test(expected = IllegalStateException.class)
    public void illegalStateOnResearchRunsWithoutDiagnosticRun() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(runApi.list(null, Ini.SOMATIC_INI, SET_ID, null, null, null, null, null))
                .thenReturn(Collections.emptyList());
        victim.handle(stagedEvent(Context.RESEARCH));
    }

    @Test
    public void errorOnResearchRunsWithNoDiagnosticRunSnpcheck() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(runApi.list(null, Ini.SOMATIC_INI, SET_ID, null, null, null, null, null))
                .thenReturn(List.of(new Run().status(Status.VALIDATED).context("RESEARCH"),
                        new Run().context("DIAGNOSTIC").failure(new RunFailure().type(TypeEnum.QCFAILURE).source("SnpCheck"))));
        victim.handle(stagedEvent(Context.RESEARCH));
        verify(validatedTopicPublisher, never()).publish(any());
    }

    @Test
    public void passesThruWhenFlagSet() {
        victim = new SnpCheck(runApi,
                sampleApi,
                pipelineStorage,
                snpcheckBucket,
                vcfComparison,
                turquoiseTopicPublisher,
                validatedTopicPublisher,
                OBJECT_MAPPER, true, false);
        when(runApi.get(run.getId())).thenReturn(run.ini(Ini.RERUN_INI.getValue()));
        victim.handle(stagedEvent(Context.RESEARCH));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        PipelineValidated validated = readEvent(pubsubMessageArgumentCaptor, PipelineValidated.class);
        assertWrappedOriginalEvent(validated, Context.RESEARCH);
        assertValidatedInApi();
    }

    private void assertValidatedInApi() {
        ArgumentCaptor<UpdateRun> updateRunArgumentCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        verify(runApi).update(eq(RUN_ID), updateRunArgumentCaptor.capture());
        assertThat(updateRunArgumentCaptor.getValue().getStatus()).isEqualTo(Status.VALIDATED);
    }

    private <T> T readEvent(final ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor, final Class<T> valueType) {
        try {
            return OBJECT_MAPPER.readValue(new String(pubsubMessageArgumentCaptor.getValue().getData().toByteArray()), valueType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertWrappedOriginalEvent(PipelineValidated event, Context context) {
        Pipeline pipeline = event.pipeline();
        Analysis analysis = pipeline.analyses().get(0);
        assertThat(pipeline.context()).isEqualTo(context);
        assertThat(pipeline.sample()).isEqualTo("samplet");
        assertThat(pipeline.version()).isEqualTo("version");
        assertThat(pipeline.runId()).isEqualTo(1);
        assertThat(pipeline.setId()).isEqualTo(2);
        assertThat(analysis.type()).isEqualTo(Type.SOMATIC);
        assertThat(analysis.molecule()).isEqualTo(Molecule.DNA);
        assertThat(analysis.output().get(0).filesize()).isEqualTo(11);
        assertThat(analysis.output().get(0).barcode()).hasValue("bc");
    }

    private void setupSnpcheckPassAndVerifyApiAndEventUpdates(PipelineComplete event) {
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        setupValidationVcfs(Result.PASS, run, BARCODE);
        victim.handle(event);
        verify(vcfComparison).compare(any(), any(), any(), any());
        verify(runApi).update(eq(RUN_ID), any(UpdateRun.class));
    }

    private void setupSnpcheckFailAndVerifyApiUpdateButNoEvents(PipelineComplete event) {
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        setupValidationVcfs(Result.FAIL, run, BARCODE);
        victim.handle(event);
        verify(validatedTopicPublisher, never()).publish(any());
        verify(runApi).update(eq(RUN_ID), any(UpdateRun.class));
    }

    private void setupHealthcheckPassAndVerifyNoApiUpdateButEvent(PipelineComplete event) {
        setupHealthcheckAndVerifyNoApiUpdateButEvent(event);
    }

    private void setupHealthcheckAndVerifyNoApiUpdateButEvent(PipelineComplete event) {
        run = run.status(Status.FAILED).failure(new RunFailure().type(TypeEnum.QCFAILURE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        setupValidationVcfs(Result.PASS, run, BARCODE);
        victim.handle(event);
        verify(runApi, never()).update(any(), any());
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        assertWrappedOriginalEvent(readEvent(pubsubMessageArgumentCaptor, PipelineValidated.class), Context.DIAGNOSTIC);
    }

    private void handleAndVerifyNoApiOrEventUpdates(PipelineComplete event) {
        victim.handle(event);
        verify(runApi, never()).update(any(), any());
        verify(vcfComparison, never()).compare(any(), any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private Page<Blob> mockPage() {
        return mock(Page.class);
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

    private void fullSnpcheckWithResult(final VcfComparison.Result result, final String barcode) {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        setupValidationVcfs(result, run, barcode);
    }

    private void setupValidationVcfs(final VcfComparison.Result result, final Run run, final String barcode) {
        Page<Blob> page = mockPage();
        Blob validationVcf = mock(Blob.class);
        when(validationVcf.getName()).thenReturn("/path/" + barcode + ".vcf");
        when(page.iterateAll()).thenReturn(singletonList(validationVcf));
        when(pipelineStorage.list(snpcheckBucket, Storage.BlobListOption.prefix(SnpCheck.SNPCHECK_VCFS))).thenReturn(page);
        Bucket referenceBucket = mock(Bucket.class);
        when(pipelineStorage.get(this.run.getBucket())).thenReturn(referenceBucket);
        Blob referenceVcf = mock(Blob.class);
        when(referenceBucket.get("set/sampler/snp_genotype/snp_genotype_output.vcf")).thenReturn(referenceVcf);
        when(vcfComparison.compare(run, referenceVcf, validationVcf, Boolean.FALSE)).thenReturn(result);
    }

    private PipelineComplete stagedEvent(final Pipeline.Context context) {
        return PipelineComplete.builder()
                .pipeline(Pipeline.builder()
                        .bucket("bucket")
                        .sample(TUMOR_SAMPLE.getName())
                        .version("version")
                        .context(context)
                        .addAnalyses(Analysis.builder().molecule(Molecule.DNA).output(List.of(outputBlob)).type(Type.SOMATIC).build())
                        .setId(SET_ID)
                        .runId(RUN_ID)
                        .build())
                .build();
    }
}
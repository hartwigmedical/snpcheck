package com.hartwig.snpcheck;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
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
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.events.Analysis;
import com.hartwig.events.Analysis.Context;
import com.hartwig.events.Analysis.Molecule;
import com.hartwig.events.Analysis.Type;
import com.hartwig.events.ImmutablePipelineStaged;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.snpcheck.VcfComparison.Result;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import junit.framework.AssertionFailedError;

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
    private Bucket snpcheckBucket;
    private VcfComparison vcfComparison;
    private Publisher turquoiseTopicPublisher;
    private Publisher validatedTopicPublisher;
    private SnpCheck victim;
    private PipelineOutputBlob outputBlob;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new Jdk8Module());
    }

    @Before
    public void setUp() {
        run = run(Ini.SOMATIC_INI);
        runApi = mock(RunApi.class);
        sampleApi = mock(SampleApi.class);
        pipelineStorage = mock(Storage.class);
        snpcheckBucket = mock(Bucket.class);
        vcfComparison = mock(VcfComparison.class);
        turquoiseTopicPublisher = mock(Publisher.class);
        validatedTopicPublisher = mock(Publisher.class);
        //noinspection unchecked
        when(turquoiseTopicPublisher.publish(any())).thenReturn(mock(ApiFuture.class));
        when(runApi.get(RUN_ID)).thenReturn(run);
        outputBlob = PipelineOutputBlob.builder()
                .barcode("bc")
                .bucket("bucket")
                .root("root")
                .filename("filename")
                .filesize(11)
                .hash("hash")
                .build();
        victim = new SnpCheck(runApi,
                sampleApi,
                snpcheckBucket,
                pipelineStorage,
                vcfComparison,
                turquoiseTopicPublisher,
                validatedTopicPublisher,
                OBJECT_MAPPER);
    }

    private Run run(final Ini somaticIni) {
        return new Run().bucket("bucket")
                .id(RUN_ID)
                .set(new RunSet().name("set").refSample("ref").id(SET_ID))
                .ini(somaticIni.getValue())
                .status(Status.FINISHED);
    }

    @Test
    public void filtersShallowSecondaryEvents() {
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.SECONDARY, Context.SHALLOW));
    }

    @Test
    public void filtersShallowGermlineEvents() {
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.GERMLINE, Context.SHALLOW));
    }

    @Test
    public void filtersShallowTertiaryEvents() {
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.TERTIARY, Context.SHALLOW));
    }

    @Test
    public void processesDiagnosticTertiaryAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
    }

    @Test
    public void processesResearchSecondaryAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.SECONDARY, Context.RESEARCH));
    }

    @Test
    public void processesResearchGermlineAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.GERMLINE, Context.RESEARCH));
    }

    @Test
    public void processesResearchTertiaryAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.TERTIARY, Context.RESEARCH));
    }

    @Test
    public void processesServicesSecondaryAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.SECONDARY, Context.SERVICES));
    }

    @Test
    public void processesServicesGermlineAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.GERMLINE, Context.SERVICES));
    }

    @Test
    public void processesServicesTertiaryAnalysis() {
        setupSnpcheckPassAndVerifyApiAndEventUpdates(stagedEvent(Type.TERTIARY, Context.SERVICES));
    }

    @Test
    public void passesThroughEventsForValidatedRuns() throws Exception {
        when(runApi.get(run.getId())).thenReturn(run.status(Status.VALIDATED));
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        assertWrappedOriginalEvent(extractEventWithKey(pubsubMessageArgumentCaptor.getAllValues(), "originalEvent"));
        verify(runApi, never()).update(any(), any());
        verify(vcfComparison, never()).compare(any(), any(), any());
    }

    @Test
    public void filtersRunsThatAreNeitherSomaticNorSingleIniRuns() {
        when(runApi.get(run.getId())).thenReturn(new Run().ini(Ini.RERUN_INI.getValue()).status(Status.FINISHED));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
    }

    @Test
    public void filtersNonFinishedSomaticRuns() {
        when(runApi.get(run.getId())).thenReturn(run.status(Status.PENDING));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
    }

    @Test
    public void filtersNonFinishedSingleRuns() {
        when(runApi.get(run.getId())).thenReturn(run.status(Status.PENDING).ini(Ini.SINGLESAMPLE_INI.getValue()));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfDiagnosticSecondary() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.SECONDARY, Context.DIAGNOSTIC));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfDiagnosticGermline() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.GERMLINE, Context.DIAGNOSTIC));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfDiagnosticTertiary() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfResearchSecondary() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.SECONDARY, Context.RESEARCH));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfResearchGermline() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.GERMLINE, Context.RESEARCH));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfResearchTertiary() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.TERTIARY, Context.RESEARCH));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfServicesSecondary() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.SECONDARY, Context.SERVICES));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfServicesGermline() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.GERMLINE, Context.SERVICES));
    }

    @Test
    public void noEventPublishedOnSnpCheckFailureOfServicesTertiary() {
        setupSnpcheckFailAndVerifyApiUpdateButNoEvents(stagedEvent(Type.TERTIARY, Context.SERVICES));
    }

    @Test
    public void finishedSomaticRunNoRefSampleMarksRunTechnicalFail() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(emptyList());
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        assertTechnicalFailure();
    }

    @Test
    public void waitsForProcessingRunsForFiveSeconds() {
        when(runApi.get(run.getId())).thenReturn(run(Ini.SOMATIC_INI).status(Status.PROCESSING))
                .thenReturn(run(Ini.SOMATIC_INI).status(Status.FINISHED));
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        verify(runApi, times(1)).update(any(), any());
    }

    @Test
    public void finishedSomaticRunNoValidationVcfDoesNothing() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        handleAndVerifyNoApiOrEventUpdates(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
    }

    @Test
    public void finishedSomaticRunNoRefVcfMarksRunTechnicalFail() {
        when(runApi.get(run.getId())).thenReturn(run);
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        Page<Blob> page = mockPage();
        Blob validationVcf = mock(Blob.class);
        when(validationVcf.getName()).thenReturn(BARCODE + ".vcf");
        when(page.iterateAll()).thenReturn(singletonList(validationVcf));
        when(snpcheckBucket.list(Storage.BlobListOption.prefix(SnpCheck.SNPCHECK_VCFS))).thenReturn(page);
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        assertTechnicalFailure();
    }

    @Test
    public void finishedSomaticDiagnosticRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticDiagnosticWithSampleInBarcodeRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE + "_sampler");
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticServicesRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Type.TERTIARY, Context.SERVICES));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticRunComparedToValidationVcfFail() {
        fullSnpcheckWithResult(VcfComparison.Result.FAIL, BARCODE);
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
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
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void publishesTurquoiseEventOnCompletion() throws Exception {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(turquoiseTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        Map<Object, Object> message = extractEventWithKey(pubsubMessageArgumentCaptor.getAllValues(), "type");
        assertThat(message.get("type")).isEqualTo("snpcheck.completed");
        List<Map<String, String>> subjects = (List<Map<String, String>>) message.get("subjects");
        assertThat(subjects.get(0).get("name")).isEqualTo("samplet");
        assertThat(subjects.get(0).get("type")).isEqualTo("sample");
    }

    @Test
    public void publishesPipelineValidatedEventOnCompletion() throws Exception {
        fullSnpcheckWithResult(VcfComparison.Result.PASS, BARCODE);
        victim.handle(stagedEvent(Type.TERTIARY, Context.DIAGNOSTIC));
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(validatedTopicPublisher, times(1)).publish(pubsubMessageArgumentCaptor.capture());
        assertWrappedOriginalEvent(extractEventWithKey(pubsubMessageArgumentCaptor.getAllValues(), "originalEvent"));
    }

    private void setupSnpcheckPassAndVerifyApiAndEventUpdates(PipelineStaged event) {
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        setupValidationVcfs(Result.PASS, run, BARCODE);
        victim.handle(event);
        verify(vcfComparison).compare(any(), any(), any());
        verify(runApi).update(eq(RUN_ID), any(UpdateRun.class));
    }

    private void setupSnpcheckFailAndVerifyApiUpdateButNoEvents(PipelineStaged event) {
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(singletonList(TUMOR_SAMPLE));
        setupValidationVcfs(Result.FAIL, run, BARCODE);
        victim.handle(event);
        verify(validatedTopicPublisher, never()).publish(any());
        verify(runApi).update(eq(RUN_ID), any(UpdateRun.class));
    }

    private void handleAndVerifyNoApiOrEventUpdates(PipelineStaged event) {
        victim.handle(event);
        verify(runApi, never()).update(any(), any());
        verify(vcfComparison, never()).compare(any(), any(), any());
    }

    private Map<Object, Object> extractEventWithKey(List<PubsubMessage> allMessages, String telltaleKey) throws Exception {
        for (PubsubMessage rawMessage : allMessages) {
            Map<Object, Object> message = OBJECT_MAPPER.readValue(new String(rawMessage.getData().toByteArray()), new TypeReference<>() {
            });
            if (message.containsKey(telltaleKey)) {
                return message;
            }
        }
        throw new AssertionFailedError(format("No message with key \"%s\"!", telltaleKey));
    }

    @SuppressWarnings("unchecked")
    private void assertWrappedOriginalEvent(Map<Object, Object> message) {
        Map<Object, Object> wrappedMessage = (Map<Object, Object>) message.get("originalEvent");
        assertThat(wrappedMessage.get("analysisType")).isEqualTo(Type.TERTIARY.toString());
        assertThat(wrappedMessage.get("analysisContext")).isEqualTo(Context.DIAGNOSTIC.toString());
        assertThat(wrappedMessage.get("sample")).isEqualTo("samplet");
        assertThat(wrappedMessage.get("analysisMolecule")).isEqualTo("DNA");
        assertThat(wrappedMessage.get("version")).isEqualTo("version");
        assertThat(wrappedMessage.get("runId")).isEqualTo(1);
        assertThat(wrappedMessage.get("setId")).isEqualTo(2);
        List<Map<String, Object>> blobs = (List<Map<String, Object>>) wrappedMessage.get("blobs");
        assertThat(blobs.get(0).get("filesize")).isEqualTo(11);
        assertThat(blobs.get(0).get("barcode")).isEqualTo("bc");
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
        when(snpcheckBucket.list(Storage.BlobListOption.prefix(SnpCheck.SNPCHECK_VCFS))).thenReturn(page);
        Bucket referenceBucket = mock(Bucket.class);
        when(pipelineStorage.get(this.run.getBucket())).thenReturn(referenceBucket);
        Blob referenceVcf = mock(Blob.class);
        when(referenceBucket.get("set/sampler/snp_genotype/snp_genotype_output.vcf")).thenReturn(referenceVcf);
        when(vcfComparison.compare(run, referenceVcf, validationVcf)).thenReturn(result);
    }

    private ImmutablePipelineStaged stagedEvent(final Analysis.Type type, final Analysis.Context context) {
        return PipelineStaged.builder()
                .analysisType(type)
                .sample(TUMOR_SAMPLE.getName())
                .version("version")
                .analysisContext(context)
                .analysisMolecule(Molecule.DNA)
                .setId(SET_ID)
                .runId(RUN_ID)
                .blobs(List.of(outputBlob))
                .build();
    }
}
package com.hartwig.snpcheck;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SnpCheckTest {

    private static final long SET_ID = 2L;
    private static final Run RUN = new Run().bucket("bucket").id(1L).set(new RunSet().name("set").refSample("ref").id(SET_ID));
    private static final String BARCODE = "barcode";
    private static final Sample REF_SAMPLE = new Sample().barcode(BARCODE).name("sampler");
    private static final Sample TUMOR_SAMPLE = new Sample().barcode(BARCODE).name("samplet");
    private RunApi runApi;
    private SampleApi sampleApi;
    private Storage pipelineStorage;
    private Bucket snpcheckBucket;
    private VcfComparison vcfComparison;
    private Publisher publisher;
    private SnpCheck victim;

    @Before
    public void setUp() {
        runApi = mock(RunApi.class);
        sampleApi = mock(SampleApi.class);
        pipelineStorage = mock(Storage.class);
        snpcheckBucket = mock(Bucket.class);
        vcfComparison = mock(VcfComparison.class);
        publisher = mock(Publisher.class);
        //noinspection unchecked
        when(publisher.publish(any())).thenReturn(mock(ApiFuture.class));
        victim = new SnpCheck(runApi, sampleApi, snpcheckBucket, pipelineStorage, vcfComparison, publisher);
        when(runApi.list(Status.FINISHED, Ini.SINGLE_INI)).thenReturn(emptyList());
        when(runApi.list(Status.FINISHED, Ini.SOMATIC_INI)).thenReturn(emptyList());
    }

    @Test
    public void noFinishedSomaticOrSingleRunsDoesNothing() {
        victim.run();
        verify(runApi, never()).update(any(), any());
    }

    @Test
    public void finishedSomaticRunNoRefSampleMarksRunTechnicalFail() {
        when(runApi.list(Status.FINISHED, Ini.SOMATIC_INI)).thenReturn(singletonList(RUN));
        when(sampleApi.list(null, SET_ID, SampleType.REF)).thenReturn(emptyList());
        victim.run();
        assertTechnicalFailure();
    }

    @Test
    public void finishedSomaticRunNoValidationVcfDoesNothing() {
        when(runApi.list(Status.FINISHED, Ini.SOMATIC_INI)).thenReturn(singletonList(RUN));
        when(sampleApi.list(null, SET_ID, SampleType.REF)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, SET_ID, SampleType.TUMOR)).thenReturn(singletonList(TUMOR_SAMPLE));
        victim.run();
        verify(runApi, never()).update(any(), any());
    }

    @Test
    public void finishedSomaticRunNoRefVcfMarksRunTechnicalFail() {
        when(runApi.list(Status.FINISHED, Ini.SOMATIC_INI)).thenReturn(singletonList(RUN));
        when(sampleApi.list(null, SET_ID, SampleType.REF)).thenReturn(singletonList(REF_SAMPLE));
        Page<Blob> page = mockPage();
        Blob validationVcf = mock(Blob.class);
        when(validationVcf.getName()).thenReturn(BARCODE + ".vcf");
        when(page.iterateAll()).thenReturn(singletonList(validationVcf));
        when(snpcheckBucket.list(Storage.BlobListOption.prefix(SnpCheck.SNPCHECK_VCFS))).thenReturn(page);
        victim.run();
        assertTechnicalFailure();
    }

    @Test
    public void finishedSomaticRunComparedToValidationVcfPass() {
        fullSnpcheckWithResult(VcfComparison.Result.PASS);
        victim.run();
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.VALIDATED);
    }

    @Test
    public void finishedSomaticRunComparedToValidationVcfFail() {
        fullSnpcheckWithResult(VcfComparison.Result.FAIL);
        victim.run();
        UpdateRun update = captureUpdate();
        assertThat(update.getStatus()).isEqualTo(Status.FAILED);
        assertThat(update.getFailure()).isEqualTo(new RunFailure().source("SnpCheck").type(RunFailure.TypeEnum.QCFAILURE));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void publishesTurquoiseEventOnCompletion() throws Exception {
        fullSnpcheckWithResult(VcfComparison.Result.PASS);
        victim.run();
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(publisher).publish(pubsubMessageArgumentCaptor.capture());
        Map<Object, Object> message =
                new ObjectMapper().readValue(new String(pubsubMessageArgumentCaptor.getValue().getData().toByteArray()),
                        new TypeReference<Map<Object, Object>>() {
                        });
        assertThat(message.get("type")).isEqualTo("snpcheck.completed");
        List<Map<String, String>> subjects = (List<Map<String, String>>) message.get("subjects");
        assertThat(subjects.get(0).get("name")).isEqualTo("samplet");
        assertThat(subjects.get(0).get("type")).isEqualTo("sample");
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
        verify(runApi).update(eq(RUN.getId()), updateRunArgumentCaptor.capture());
        return updateRunArgumentCaptor.getValue();
    }

    private void fullSnpcheckWithResult(final VcfComparison.Result result) {
        when(runApi.list(Status.FINISHED, Ini.SOMATIC_INI)).thenReturn(singletonList(RUN));
        when(sampleApi.list(null, SET_ID, SampleType.REF)).thenReturn(singletonList(REF_SAMPLE));
        when(sampleApi.list(null, SET_ID, SampleType.TUMOR)).thenReturn(singletonList(TUMOR_SAMPLE));
        Page<Blob> page = mockPage();
        Blob validationVcf = mock(Blob.class);
        when(validationVcf.getName()).thenReturn(BARCODE + ".vcf");
        when(page.iterateAll()).thenReturn(singletonList(validationVcf));
        when(snpcheckBucket.list(Storage.BlobListOption.prefix(SnpCheck.SNPCHECK_VCFS))).thenReturn(page);
        Bucket referenceBucket = mock(Bucket.class);
        when(pipelineStorage.get(RUN.getBucket())).thenReturn(referenceBucket);
        Blob referenceVcf = mock(Blob.class);
        when(referenceBucket.get("set/sampler/snp_genotype/snp_genotype_output.vcf")).thenReturn(referenceVcf);
        when(vcfComparison.compare(RUN, referenceVcf, validationVcf)).thenReturn(result);
    }
}
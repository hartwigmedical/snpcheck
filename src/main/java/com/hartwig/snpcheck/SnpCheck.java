package com.hartwig.snpcheck;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
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
import com.hartwig.events.Handler;
import com.hartwig.events.PipelineComplete;
import com.hartwig.events.PipelineValidated;
import com.hartwig.snpcheck.VcfComparison.Result;
import com.hartwig.snpcheck.turquoise.SnpCheckEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnpCheck implements Handler<PipelineComplete> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheck.class);
    public static final String SNPCHECK_VCFS = "snpcheckvcfs";
    public static final String SNP_CHECK = "SnpCheck";

    private final RunApi runs;
    private final SampleApi samples;
    private final Bucket snpcheckBucket;
    private final Storage pipelineStorage;
    private final VcfComparison vcfComparison;
    private final Publisher turquoiseTopicPublisher;
    private final Publisher validatedTopicPublisher;
    private final ObjectMapper objectMapper;
    private final LabPendingBuffer labPendingBuffer;
    private final boolean passthru;

    public SnpCheck(final RunApi runs, final SampleApi samples, final Bucket snpcheckBucket, final Storage pipelineStorage,
                    final VcfComparison vcfComparison, final Publisher publisher, final Publisher validatedTopicPublisher,
                    final ObjectMapper objectMapper, final boolean passthru) {
        this.runs = runs;
        this.samples = samples;
        this.snpcheckBucket = snpcheckBucket;
        this.pipelineStorage = pipelineStorage;
        this.vcfComparison = vcfComparison;
        this.turquoiseTopicPublisher = publisher;
        this.validatedTopicPublisher = validatedTopicPublisher;
        this.objectMapper = objectMapper;
        this.passthru = passthru;
        this.labPendingBuffer = new LabPendingBuffer(this, Executors.newScheduledThreadPool(1), TimeUnit.HOURS, 1);
    }

    public void handle(final PipelineComplete event) {
        try {
            Run run = runs.get(event.pipeline().runId());
            if (passthru) {
                LOGGER.info("Passing through event for sample [{}]", event.pipeline().sample());
                publishValidated(event);
            } else if (run.getIni().equals(Ini.SOMATIC_INI.getValue()) || run.getIni().equals(Ini.SINGLESAMPLE_INI.getValue())) {
                LOGGER.info("Received a SnpCheck candidate [{}] for run [{}]", run.getSet().getName(), run.getId());
                if (run.getStatus() == Status.FINISHED || runFailedQc(run)) {
                    Iterable<Blob> valVcfs = Optional.ofNullable(snpcheckBucket.list(Storage.BlobListOption.prefix(SNPCHECK_VCFS)))
                            .map(Page::iterateAll)
                            .orElse(Collections.emptyList());
                    Optional<Sample> maybeRefSample = onlyOne(samples, run.getSet(), SampleType.REF);
                    Optional<Sample> maybeTumorSample = onlyOne(samples, run.getSet(), SampleType.TUMOR);
                    if (maybeRefSample.isPresent()) {
                        Sample refSample = maybeRefSample.get();
                        Optional<Blob> maybeValVcf = findValidationVcf(valVcfs, refSample);
                        if (maybeValVcf.isPresent()) {
                            VcfComparison.Result result = doComparison(run, refSample, maybeValVcf.get());
                            SnpCheckEvent.builder()
                                    .publisher(turquoiseTopicPublisher)
                                    .sample(maybeTumorSample.map(Sample::getName).orElse(refSample.getName()))
                                    .result(result.name().toLowerCase())
                                    .build()
                                    .publish();
                            if (result.equals(Result.PASS)) {
                                publishValidated(event);
                            }
                        } else {
                            LOGGER.info("No validation VCF available for set [{}].", run.getSet().getName());
                            labPendingBuffer.add(event);
                        }
                    } else {
                        LOGGER.warn("Set [{}] had no ref sample available in the API. Unable to locate validation VCF.",
                                run.getSet().getName());
                        failed(run, RunFailure.TypeEnum.TECHNICALFAILURE);
                    }
                } else {
                    LOGGER.info("Skipping run with status [{}]", run.getStatus());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to process SnpCheck", e);
        }
    }

    private void publishValidated(PipelineComplete event) {
        PipelineValidated.builder().pipeline(event.pipeline()).build().publish(validatedTopicPublisher, objectMapper);
    }

    private void failed(final Run run, final RunFailure.TypeEnum failure) {
        runs.update(run.getId(), new UpdateRun().status(Status.FAILED).failure(new RunFailure().source(SNP_CHECK).type(failure)));
    }

    private static Optional<Blob> findValidationVcf(final Iterable<Blob> valVcfs, final Sample refSample) {
        String barcode = refSample.getBarcode().split("_")[0];
        return StreamSupport.stream(valVcfs.spliterator(), false).filter(vcf -> {
            String[] splitted = vcf.getName().split("/");
            String fileName = splitted[splitted.length - 1];
            return fileName.startsWith(barcode);
        }).findFirst();
    }

    private VcfComparison.Result doComparison(final Run run, final Sample refSample, final Blob valVcf) {
        String refVcfPath = String.format("%s/%s/snp_genotype/snp_genotype_output.vcf", run.getSet().getName(), refSample.getName());
        Optional<Blob> maybeRefVcf =
                Optional.ofNullable(pipelineStorage.get(run.getBucket())).flatMap(b -> Optional.ofNullable(b.get(refVcfPath)));
        if (maybeRefVcf.isPresent()) {
            Blob refVcf = maybeRefVcf.get();
            LOGGER.info("Found both a validation and reference VCF for set [{}]", run.getSet().getName());
            LOGGER.info("Validation [{}]", valVcf.getName());
            LOGGER.info("Reference [{}]", refVcf.getName());
            VcfComparison.Result result = vcfComparison.compare(run, refVcf, valVcf);
            if (result.equals(VcfComparison.Result.PASS)) {
                LOGGER.info("Set [{}] was successfully snpchecked.", run.getSet().getName());
                if (!runFailedQc(run)) {
                    runs.update(run.getId(), new UpdateRun().status(Status.VALIDATED));
                }
            } else {
                LOGGER.info("Set [{}] failed snpcheck.", run.getSet().getName());
                failed(run, RunFailure.TypeEnum.QCFAILURE);
            }
            return result;
        } else {
            LOGGER.warn("Set [{}] had no VCF at [{}]", run.getSet().getName(), refVcfPath);
            failed(run, RunFailure.TypeEnum.TECHNICALFAILURE);
            return VcfComparison.Result.FAIL;
        }
    }

    private boolean runFailedQc(final Run run) {
        return run.getStatus() == Status.FAILED && (run.getFailure() != null && run.getFailure().getType() == TypeEnum.QCFAILURE);
    }

    private static Optional<Sample> onlyOne(final SampleApi api, RunSet set, SampleType type) {
        List<Sample> samples = api.list(null, null, null, set.getId(), type, null);
        if (samples.size() > 1) {
            throw new IllegalStateException(String.format("Multiple samples found for type [%s] and set [%s]", type, set.getName()));
        }
        return samples.stream().findFirst();
    }
}

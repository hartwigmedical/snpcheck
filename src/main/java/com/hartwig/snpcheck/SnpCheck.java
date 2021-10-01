package com.hartwig.snpcheck;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
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
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.events.Analysis.Context;
import com.hartwig.events.Analysis.Type;
import com.hartwig.events.Handler;
import com.hartwig.events.PipelineStaged;
import com.hartwig.events.PipelineValidated;
import com.hartwig.snpcheck.turquoise.SnpCheckEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnpCheck implements Handler<PipelineStaged> {

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
    private final ArrayList<PipelineStaged> pendingEvents;

    public SnpCheck(final RunApi runs, final SampleApi samples, final Bucket snpcheckBucket, final Storage pipelineStorage,
            final VcfComparison vcfComparison, final Publisher publisher, final Publisher validatedTopicPublisher,
            final ObjectMapper objectMapper) {
        this.runs = runs;
        this.samples = samples;
        this.snpcheckBucket = snpcheckBucket;
        this.pipelineStorage = pipelineStorage;
        this.vcfComparison = vcfComparison;
        this.turquoiseTopicPublisher = publisher;
        this.validatedTopicPublisher = validatedTopicPublisher;
        this.objectMapper = objectMapper;
        this.labPendingBuffer = new LabPendingBuffer(this, Executors.newScheduledThreadPool(1), TimeUnit.HOURS, 1);
        this.pendingEvents = new ArrayList<>();
    }

    private Optional<List<PipelineStaged>> enqueueEvent(final PipelineStaged event) {
        pendingEvents.add(event);
        List<PipelineStaged> found = pendingEvents.stream().filter(p -> p.runId() == event.runId() && p.analysisContext().equals(event.analysisContext())).collect(toList());
        Optional<PipelineStaged> secondary = found.stream().filter(f -> f.analysisType().equals(Type.SECONDARY)).findFirst();
        Optional<PipelineStaged> germline = found.stream().filter(f -> f.analysisType().equals(Type.GERMLINE)).findFirst();
        Optional<PipelineStaged> tertiary = found.stream().filter(f -> f.analysisType().equals(Type.TERTIARY)).findFirst();
        if (secondary.isPresent() && germline.isPresent() && tertiary.isPresent()) {
            List<PipelineStaged> toDequeue = List.of(secondary.get(), germline.get(), tertiary.get());
            pendingEvents.removeAll(toDequeue);
            return Optional.of(toDequeue);
        }
        return Optional.empty();
    }

    public void handle(final PipelineStaged newEvent) {
        try {
            if (!newEvent.analysisContext().equals(Context.SHALLOW)) {
                enqueueEvent(newEvent).ifPresent(events -> {
                    for (PipelineStaged event : events) {
                        if (event.analysisType().equals(Type.TERTIARY)) {
                            Run run = runs.get(event.runId().orElseThrow());
                            if (run.getIni().equals(Ini.SOMATIC_INI.getValue()) || run.getIni().equals(Ini.SINGLESAMPLE_INI.getValue())) {
                                LOGGER.info("Received a SnpCheck candidate [{}] for run [{}]", run.getSet().getName(), run.getId());
                                if (waitForFinished(run)) {
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
                                            for (PipelineStaged original : events) {
                                                PipelineValidated.builder()
                                                        .originalEvent(original)
                                                        .build()
                                                        .publish(validatedTopicPublisher, objectMapper);
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
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("SnpCheck failed", e);
        }
    }

    private boolean waitForFinished(final Run run) {
        Status currentStatus = run.getStatus();
        if (currentStatus.equals(Status.PROCESSING)) {
            LOGGER.info("Status of run was still processing. Waiting 5s and checking again.");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            currentStatus = runs.get(run.getId()).getStatus();
            LOGGER.info("After 5s the status is [{}]", currentStatus);
        }
        return currentStatus.equals(Status.FINISHED);
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
                runs.update(run.getId(), new UpdateRun().status(Status.VALIDATED));
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

    private static Optional<Sample> onlyOne(final SampleApi api, RunSet set, SampleType type) {
        List<Sample> samples = api.list(null, null, null, set.getId(), type, null);
        if (samples.size() > 1) {
            throw new IllegalStateException(String.format("Multiple samples found for type [%s] and set [%s]", type, set.getName()));
        }
        return samples.stream().findFirst();
    }
}

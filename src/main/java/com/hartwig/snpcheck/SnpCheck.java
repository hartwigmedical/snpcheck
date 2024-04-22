package com.hartwig.snpcheck;

import static com.hartwig.events.pipeline.Pipeline.Context.DIAGNOSTIC;
import static com.hartwig.events.pipeline.Pipeline.Context.RESEARCH;
import static com.hartwig.events.pipeline.Pipeline.Context.RESEARCH2;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.helpers.OnlyOne;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.RunFailure.TypeEnum;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.events.EventHandler;
import com.hartwig.events.EventPublisher;
import com.hartwig.events.aqua.SnpCheckCompletedEvent;
import com.hartwig.events.aqua.model.AquaEvent;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pipeline.PipelineValidated;
import com.hartwig.events.turquoise.TurquoiseEvent;
import com.hartwig.events.turquoise.model.Label;
import com.hartwig.events.turquoise.model.Subject;
import com.hartwig.snpcheck.VcfComparison.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnpCheck implements EventHandler<PipelineComplete> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheck.class);
    public static final String SNPCHECK_VCFS = "snpcheckvcfs";
    public static final String SNP_CHECK = "SnpCheck";

    private final RunApi runs;
    private final SampleApi sampleApi;
    private final Storage pipelineStorage;
    private final String bucketName;
    private final VcfComparison vcfComparison;
    private final EventPublisher<TurquoiseEvent> turquoisePublisher;
    private final EventPublisher<AquaEvent> aquaPublisher;
    private final EventPublisher<PipelineValidated> validatedEventPublisher;
    private final LabPendingBuffer labPendingBuffer;
    private final boolean passthru;
    private final boolean alwaysPass;

    public SnpCheck(RunApi runs, SampleApi sampleApi, Storage pipelineStorage, String bucketName, VcfComparison vcfComparison,
            EventPublisher<TurquoiseEvent> turquoisePublisher, EventPublisher<AquaEvent> aquaPublisher,
            EventPublisher<PipelineValidated> validatedEventPublisher, boolean passthru, boolean alwaysPass) {
        this.runs = runs;
        this.sampleApi = sampleApi;
        this.pipelineStorage = pipelineStorage;
        this.bucketName = bucketName;
        this.vcfComparison = vcfComparison;
        this.turquoisePublisher = turquoisePublisher;
        this.aquaPublisher = aquaPublisher;
        this.validatedEventPublisher = validatedEventPublisher;
        this.passthru = passthru;
        this.alwaysPass = alwaysPass;
        this.labPendingBuffer = new LabPendingBuffer(this, Executors.newScheduledThreadPool(1), TimeUnit.HOURS, 1);
    }

    public void handle(final PipelineComplete event) {
        Run run = runs.get(event.pipeline().runId());
        if (passthru) {
            LOGGER.info("Passing through event for sample [{}]", event.pipeline().sample());
            publishAndUpdateApiValidated(event, run);
        } else if (run.getIni().equals(Ini.SOMATIC_INI.getValue()) || run.getIni().equals(Ini.SINGLESAMPLE_INI.getValue())) {
            LOGGER.info("Received a SnpCheck candidate [{}] for run [{}]", run.getSet().getName(), run.getId());
            if (isResearchContext(event.pipeline().context())) {
                validateResearchWhenSourceRunValidated(event, run);
            } else {
                validateRunWithSnpcheck(event, run);
            }
        }
    }

    private boolean isResearchContext(Pipeline.Context context) {
        return context.equals(RESEARCH) || context.equals(RESEARCH2);
    }

    private void validateRunWithSnpcheck(final PipelineComplete event, final Run run) {
        if (run.getStatus() != Status.FINISHED && !runFailedQc(run)) {
            LOGGER.info("Skipping run with status [{}]", run.getStatus());
            return;
        }
        Iterable<Blob> valVcfs = Optional.ofNullable(pipelineStorage.list(bucketName, Storage.BlobListOption.prefix(SNPCHECK_VCFS)))
                .map(Page::iterateAll)
                .orElse(Collections.emptyList());
        var runSet = run.getSet();
        Optional<Sample> maybeRefSample = findSample(runSet, SampleType.REF);
        Optional<Sample> maybeTumorSample = findSample(runSet, SampleType.TUMOR);
        if (maybeRefSample.isEmpty()) {
            LOGGER.warn("Set [{}] had no ref sample available in the API. Unable to locate validation VCF.", runSet.getName());
            apiFailed(run, TypeEnum.TECHNICALFAILURE);
            return;
        }
        Sample refSample = maybeRefSample.get();
        Optional<Blob> maybeValVcf = findValidationVcf(valVcfs, refSample);
        if (maybeValVcf.isEmpty()) {
            LOGGER.info("No validation VCF available for runSet [{}].", runSet.getName());
            labPendingBuffer.add(event);
            return;
        }
        Result result = doComparison(run, refSample, maybeValVcf.get());
        publishTurquoiseEvent(maybeTumorSample.map(Sample::getName).orElse(refSample.getName()), result.name());
        var aquaEvent = SnpCheckCompletedEvent.builder()
                .timestamp(Instant.now())
                .barcode(maybeTumorSample.map(Sample::getBarcode).orElse(refSample.getBarcode()))
                .snpCheckResult(result.name())
                .ini(run.getIni())
                .context(event.pipeline().context())
                .build();
        aquaPublisher.publish(aquaEvent);
        if (result.equals(Result.PASS)) {
            publishValidated(event);
        }
    }

    private void publishTurquoiseEvent(String sampleName, String result) {
        turquoisePublisher.publish(TurquoiseEvent.builder()
                .timestamp(ZonedDateTime.now())
                .type("snpcheck.completed")
                .addLabels(Label.of("result", result))
                .addSubjects(Subject.builder().type("sample").name(sampleName).labels(Collections.emptyList()).build())
                .build());
    }

    private void validateResearchWhenSourceRunValidated(final PipelineComplete event, final Run run) {
        Run mostRecentDiagnostic = runs.callList(null, Ini.SOMATIC_INI, run.getSet().getId(), null, null, null, null, null, null)
                .stream()
                .filter(r -> !isResearchContext(Pipeline.Context.valueOf(r.getContext())) && r.getEndTime() != null)
                .max(Comparator.comparing(Run::getEndTime))
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "Research run [%s] for set [%s] had no diagnostic run. Cannot validate.",
                        run.getId(),
                        run.getSet().getName())));
        var failedSnpcheck = sourceRunHasFailedSnpcheck(mostRecentDiagnostic);
        if (failedSnpcheck) {
            LOGGER.warn("Diagnostic run [{}] for research run [{}], for set [{}] has failed snpcheck. Cannot validate.",
                    mostRecentDiagnostic.getId(),
                    run.getId(),
                    run.getSet().getName());
        } else {
            LOGGER.info("Validating research run [{}], set [{}] as the diagnostic run passed snpcheck",
                    run.getId(),
                    run.getSet().getName());
            publishAndUpdateApiValidated(event, run);
        }
        var barcode = findBarcode(run);
        var result = failedSnpcheck ? Result.FAIL : Result.PASS;
        var aquaEvent = SnpCheckCompletedEvent.builder()
                .timestamp(Instant.now())
                .barcode(barcode)
                .snpCheckResult(result.name())
                .ini(run.getIni())
                .context(event.pipeline().context())
                .build();
        aquaPublisher.publish(aquaEvent);
    }

    private void publishAndUpdateApiValidated(final PipelineComplete event, final Run run) {
        publishValidated(event);
        apiValidated(run);
    }

    private static boolean sourceRunHasFailedSnpcheck(final Run source) {
        return source.getFailure() != null && source.getFailure().getSource().equals("SnpCheck");
    }

    private void publishValidated(final PipelineComplete event) {
        validatedEventPublisher.publish(PipelineValidated.builder().pipeline(event.pipeline()).build());
    }

    private void apiFailed(final Run run, final RunFailure.TypeEnum failure) {
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
        boolean isDiagnosticRun = DIAGNOSTIC.name().equalsIgnoreCase(run.getContext());
        String refSampleAnalysisName = isDiagnosticRun ? refSample.getReportingId() : refSample.getName();
        String refVcfPath = String.format("%s/%s/snp_genotype/snp_genotype_output.vcf", run.getSet().getName(), refSampleAnalysisName);
        Optional<Blob> maybeRefVcf =
                Optional.ofNullable(pipelineStorage.get(run.getBucket())).flatMap(b -> Optional.ofNullable(b.get(refVcfPath)));
        if (maybeRefVcf.isPresent()) {
            Blob refVcf = maybeRefVcf.get();
            LOGGER.info("Found both a validation and reference VCF for set [{}]", run.getSet().getName());
            LOGGER.info("Validation [{}]", valVcf.getName());
            LOGGER.info("Reference [{}]", refVcf.getName());
            VcfComparison.Result result = vcfComparison.compare(run, refVcf, valVcf, alwaysPass);
            if (result.equals(VcfComparison.Result.PASS)) {
                LOGGER.info("Set [{}] was successfully snpchecked.", run.getSet().getName());
                if (!runFailedQc(run)) {
                    apiValidated(run);
                }
            } else {
                LOGGER.info("Set [{}] failed snpcheck.", run.getSet().getName());
                apiFailed(run, RunFailure.TypeEnum.QCFAILURE);
            }
            return result;
        } else {
            LOGGER.warn("Set [{}] had no VCF at [{}]", run.getSet().getName(), refVcfPath);
            apiFailed(run, RunFailure.TypeEnum.TECHNICALFAILURE);
            return VcfComparison.Result.FAIL;
        }
    }

    private void apiValidated(final Run run) {
        runs.update(run.getId(), new UpdateRun().status(Status.VALIDATED));
    }

    private boolean runFailedQc(final Run run) {
        return run.getStatus() == Status.FAILED && (run.getFailure() != null && run.getFailure().getType() == TypeEnum.QCFAILURE);
    }

    private String findBarcode(Run run) {
        var runSet = run.getSet();
        return findSample(runSet, SampleType.TUMOR).or(() -> findSample(runSet, SampleType.REF))
                .orElseThrow(() -> new IllegalStateException(String.format("Set [%s] had no samples available in the API",
                        runSet.getName())))
                .getBarcode();
    }

    private Optional<Sample> findSample(RunSet set, SampleType type) {
        List<Sample> samples = sampleApi.callList(null, null, null, set.getId(), type, null, null);
        return OnlyOne.ofNullable(samples, Sample.class);
    }
}

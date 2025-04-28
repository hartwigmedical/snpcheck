package com.hartwig.snpcheck;

import static com.hartwig.events.pipeline.Pipeline.Context.RESEARCH;
import static com.hartwig.events.pipeline.Pipeline.Context.RESEARCH2;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pipeline.PipelineValidated;
import com.hartwig.events.turquoise.TurquoiseEvent;
import com.hartwig.events.turquoise.model.Label;
import com.hartwig.events.turquoise.model.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnpCheck implements EventHandler<PipelineComplete> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheck.class);

    private final RunApi runs;
    private final SampleApi sampleApi;
    private final EventPublisher<TurquoiseEvent> turquoisePublisher;
    private final EventPublisher<AquaEvent> aquaPublisher;
    private final EventPublisher<PipelineValidated> validatedEventPublisher;
    private final boolean passthru;

    public SnpCheck(RunApi runs, SampleApi sampleApi, EventPublisher<TurquoiseEvent> turquoisePublisher,
            EventPublisher<AquaEvent> aquaPublisher, EventPublisher<PipelineValidated> validatedEventPublisher, boolean passthru) {
        this.runs = runs;
        this.sampleApi = sampleApi;
        this.turquoisePublisher = turquoisePublisher;
        this.aquaPublisher = aquaPublisher;
        this.validatedEventPublisher = validatedEventPublisher;
        this.passthru = passthru;
    }

    public void handle(PipelineComplete event) {
        Run run = runs.get(event.pipeline().runId());
        if (passthru || event.pipeline().context() == RESEARCH || event.pipeline().context() == RESEARCH2) {
            LOGGER.info("Passing through event for sample [{}]", event.pipeline().sample());
            publishAndUpdateApiValidated(event, run);
        } else if (run.getIni().equals(Ini.SOMATIC_INI.getValue()) || run.getIni().equals(Ini.SINGLESAMPLE_INI.getValue())) {
            LOGGER.info("Received a SnpCheck candidate [{}] for run [{}]", run.getSet().getName(), run.getId());
            validateRun(event, run);
        }
    }

    private void validateRun(PipelineComplete event, Run run) {
        if (run.getStatus() != Status.FINISHED && !runFailedQc(run)) {
            LOGGER.info("Skipping run with status [{}]", run.getStatus());
            return;
        }

        var runSet = run.getSet();
        Optional<Sample> maybeRefSample = findSample(runSet, SampleType.REF);
        Optional<Sample> maybeTumorSample = findSample(runSet, SampleType.TUMOR);
        if (maybeRefSample.isEmpty()) {
            LOGGER.warn("Set [{}] had no ref sample available in the API.", runSet.getName());
            apiFailed(run);
            return;
        }
        Sample refSample = maybeRefSample.get();

        LOGGER.info("Set [{}] was successfully validated.", run.getSet().getName());
        if (!runFailedQcHealthCheck(run)) {
            apiValidated(run);
        }

        // Snpcheck used to compare the lab VCFs with the pipeline VCFs, but the lab discontinued this at 25-04-2025 in favor
        // of a check by the medical team based on Amber.
        //
        // But: snpcheck also acts as a filter for pipeline events: it would only publish pipeline.validated events for
        // pipeline.complete events with a lab VCF and a certain run status. We want to keep this filtering behavior because
        // downstream services (archiver, diagnostic-genomic-db-loader, etc.) depend on it. So the "result" of the check is
        // always "PASS", but we only publish events for certain upstream pipeline.complete events.
        var result = "PASS";
        publishTurquoiseEvent(maybeTumorSample.map(Sample::getName).orElse(refSample.getName()), result);
        var aquaEvent = SnpCheckCompletedEvent.builder()
                .timestamp(Instant.now())
                .barcode(maybeTumorSample.map(Sample::getBarcode).orElse(refSample.getBarcode()))
                .snpCheckResult(result)
                .ini(run.getIni())
                .context(event.pipeline().context())
                .build();
        aquaPublisher.publish(aquaEvent);
        publishValidated(event);
    }

    private void publishTurquoiseEvent(String sampleName, String result) {
        turquoisePublisher.publish(TurquoiseEvent.builder()
                .timestamp(ZonedDateTime.now())
                .type("snpcheck.completed")
                .addLabels(Label.of("result", result))
                .addSubjects(Subject.builder().type("sample").name(sampleName).labels(Collections.emptyList()).build())
                .build());
    }

    private void publishAndUpdateApiValidated(PipelineComplete event, Run run) {
        publishValidated(event);
        apiValidated(run);
    }

    private void publishValidated(PipelineComplete event) {
        validatedEventPublisher.publish(PipelineValidated.builder().pipeline(event.pipeline()).build());
    }

    private void apiFailed(Run run) {
        runs.update(run.getId(),
                new UpdateRun().status(Status.FAILED).failure(new RunFailure().source("SnpCheck").type(TypeEnum.TECHNICALFAILURE)));
    }

    private void apiValidated(Run run) {
        runs.update(run.getId(), new UpdateRun().status(Status.VALIDATED));
    }

    private static boolean runFailedQc(Run run) {
        return run.getStatus() == Status.FAILED && (run.getFailure() != null && run.getFailure().getType() == TypeEnum.QCFAILURE);
    }

    private static boolean runFailedQcHealthCheck(Run run) {
        return runFailedQc(run) && Objects.requireNonNull(run.getFailure()).getSource().equals("HealthCheck");
    }

    private Optional<Sample> findSample(RunSet set, SampleType type) {
        List<Sample> samples = sampleApi.callList(null, null, null, set.getId(), type, null, null);
        return OnlyOne.ofNullable(samples, Sample.class);
    }
}

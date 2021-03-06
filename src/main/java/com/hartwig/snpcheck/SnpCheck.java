package com.hartwig.snpcheck;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
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
import com.hartwig.snpcheck.turquoise.SnpCheckEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnpCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheck.class);
    public static final String SNPCHECK_VCFS = "snpcheckvcfs";
    public static final String SNP_CHECK = "SnpCheck";

    private final RunApi runs;
    private final SampleApi samples;
    private final Bucket snpcheckBucket;
    private final Storage pipelineStorage;
    private final VcfComparison vcfComparison;
    private final Publisher publisher;

    public SnpCheck(final RunApi runs, final SampleApi samples, final Bucket snpcheckBucket, final Storage pipelineStorage,
            final VcfComparison vcfComparison, final Publisher publisher) {
        this.runs = runs;
        this.samples = samples;
        this.snpcheckBucket = snpcheckBucket;
        this.pipelineStorage = pipelineStorage;
        this.vcfComparison = vcfComparison;
        this.publisher = publisher;
    }

    public void run() {
        LOGGER.info("Starting snpcheck run");
        List<Run> singles = runs.list(Status.FINISHED, Ini.SINGLE_INI);
        List<Run> somatics = runs.list(Status.FINISHED, Ini.SOMATIC_INI);
        Iterable<Blob> valVcfs = Optional.ofNullable(snpcheckBucket.list(Storage.BlobListOption.prefix(SNPCHECK_VCFS)))
                .map(Page::iterateAll)
                .orElse(Collections.emptyList());
        LOGGER.info("Found [{}] somatic and [{}] single runs to check", somatics.size(), singles.size());
        for (Run run : Iterables.concat(singles, somatics)) {
            Optional<Sample> maybeRefSample = onlyOne(samples, run.getSet(), SampleType.REF);
            Optional<Sample> maybeTumorSample = onlyOne(samples, run.getSet(), SampleType.TUMOR);
            if (maybeRefSample.isPresent() && maybeTumorSample.isPresent()) {
                Sample refSample = maybeRefSample.get();
                Sample tumorSample = maybeTumorSample.get();
                Optional<Blob> maybeValVcf = findValidationVcf(valVcfs, refSample);
                if (maybeValVcf.isPresent()) {
                    doComparison(run, refSample, tumorSample, maybeValVcf.get());
                } else {
                    LOGGER.info("No validation VCF available for set [{}]. Will be checked again next time snpcheck runs",
                            run.getSet().getName());
                }
            } else {
                LOGGER.warn("Set [{}] had no ref sample available in the API. Unable to locate validation VCF.", run.getSet().getName());
                failed(run, RunFailure.TypeEnum.TECHNICALFAILURE);
            }
        }
    }

    private void failed(final Run run, final RunFailure.TypeEnum failure) {
        runs.update(run.getId(), new UpdateRun().status(Status.FAILED).failure(new RunFailure().source(SNP_CHECK).type(failure)));
    }

    private static Optional<Blob> findValidationVcf(final Iterable<Blob> valVcfs, final Sample refSample) {
        return StreamSupport.stream(valVcfs.spliterator(), false).filter(vcf -> vcf.getName().contains(refSample.getBarcode())).findFirst();
    }

    private void doComparison(final Run run, final Sample refSample, final Sample tumorSample, final Blob valVcf) {
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
            SnpCheckEvent.builder()
                    .publisher(publisher)
                    .sample(tumorSample.getName())
                    .result(result.name().toLowerCase())
                    .build()
                    .publish();

        } else {
            LOGGER.warn("Set [{}] had no VCF at [{}]", run.getSet().getName(), refVcfPath);
            failed(run, RunFailure.TypeEnum.TECHNICALFAILURE);
        }
    }

    private static Optional<Sample> onlyOne(final SampleApi api, RunSet set, SampleType type) {
        List<Sample> samples = api.list(null, set.getId(), type);
        if (samples.size() > 1) {
            throw new IllegalStateException(String.format("Multiple samples found for type [%s] and set [%s]", type, set.getName()));
        }
        return samples.stream().findFirst();
    }
}

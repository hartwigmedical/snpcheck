package com.hartwig.snpcheck;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.cli.options.ApiOptions;
import com.hartwig.cli.options.PubsubOptions;
import com.hartwig.cli.options.TurquoiseOptions;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pipeline.PipelineValidated;
import com.hartwig.events.pubsub.EventPublisher;
import com.hartwig.events.pubsub.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;

public class SnpCheckMain implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheckMain.class);

    @CommandLine.Mixin
    private final ApiOptions apiOptions = new ApiOptions();

    @CommandLine.Mixin
    private final PubsubOptions pubsubOptions = new PubsubOptions();

    @CommandLine.Mixin
    private final TurquoiseOptions turquoiseOptions = new TurquoiseOptions();

    @CommandLine.Option(names = {"--snpcheck_bucket"},
            defaultValue = "hmf-snpcheck",
            description = "Bucket in which the snpcheck vcfs are uploaded")
    private String snpcheckBucketName;

    @CommandLine.Option(names = {"--project"},
            required = true,
            description = "Project in which the snpcheck is running")
    private String project;

    @CommandLine.Option(names = {"--passthru"},
            defaultValue = "false",
            description = "Mark all events as validated without actually validating against the snpcheck vcf.")
    private boolean passthru;

    @CommandLine.Option(names = {"--always_pass"},
            defaultValue = "false",
            description = "Run the snpcheck script in always pass mode.")
    private boolean alwaysPass;

    @Override
    public Integer call() {
        try {
            if (passthru && project.contains("prod")) {
                LOGGER.error("Snpcheck does not allow configuring passthru on a production project.");
                return 1;
            }
            LOGGER.info("Snpcheck configured to alwaysPass={} mode.", alwaysPass);
            EventSubscriber<PipelineComplete> subscriber = pubsubOptions.eventSubscriber(new PipelineComplete.EventDescriptor(), "snpcheck");
            EventPublisher<PipelineValidated> publisher = pubsubOptions.eventPublisher(new PipelineValidated.EventDescriptor());
            subscriber.subscribe(new SnpCheck(apiOptions.api().runs(),
                    apiOptions.api().samples(),
                    StorageOptions.getDefaultInstance().getService(),
                    snpcheckBucketName,
                    new PerlVcfComparison(),
                    turquoiseOptions.publisher(),
                    publisher,
                    passthru, alwaysPass));
            return 0;
        } catch (Exception e) {
            LOGGER.error("Exception while running snpcheck", e);
            return 1;
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new SnpCheckMain()).execute(args));
    }
}

package com.hartwig.snpcheck;

import java.util.concurrent.Callable;

import com.google.cloud.storage.StorageOptions;
import com.hartwig.api.HmfApi;
import com.hartwig.events.EventBuilder;
import com.hartwig.events.EventPublisher;
import com.hartwig.events.aqua.model.AquaEvent;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pipeline.PipelineValidated;
import com.hartwig.events.pubsub.PubsubEventBuilder;
import com.hartwig.events.turquoise.TurquoiseEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

public class SnpCheckMain implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheckMain.class);

    @CommandLine.Option(names = { "--api_url" },
                        description = { "URL to use for HMF API interactions, including protocol and port" },
                        defaultValue = "http://api")
    protected String apiUrl;

    @CommandLine.Option(names = { "--turquoise_project" })
    protected String turquoiseProject;

    @CommandLine.Option(names = { "--aqua_project" })
    protected String aquaProject;

    @CommandLine.Option(names = { "--snpcheck_bucket" },
                        defaultValue = "hmf-snpcheck",
                        description = "Bucket in which the snpcheck vcfs are uploaded")
    private String snpcheckBucketName;

    @CommandLine.Option(names = { "--project" },
                        required = true,
                        description = "Project in which the snpcheck is running")
    private String project;

    @CommandLine.Option(names = { "--passthru" },
                        defaultValue = "false",
                        description = "Mark all events as validated without invoking the Perl script (contrast with --always-pass).")
    private boolean passthru;

    @CommandLine.Option(names = { "--always_pass" },
                        defaultValue = "false",
                        description = "Invoke the Perl script to check the lab VCF exists but assume the contents is correct (contrast with --passthru).")
    private boolean alwaysPass;

    @Override
    public Integer call() {
        try {
            LOGGER.info("Snpcheck configured to alwaysPass={} mode.", alwaysPass);
            var pubsubEventBuilder = new PubsubEventBuilder();
            var publisher = pubsubEventBuilder.newPublisher(project, new PipelineValidated.EventDescriptor());
            var subscriber = pubsubEventBuilder.newSubscriber(project, new PipelineComplete.EventDescriptor(), "snpcheck", 1, true);
            EventPublisher<TurquoiseEvent> turquoisePublisher = turquoiseProject == null
                    ? EventBuilder.noopPublisher()
                    : pubsubEventBuilder.newPublisher(turquoiseProject, new TurquoiseEvent.EventDescriptor());
            EventPublisher<AquaEvent> aquaPublisher = aquaProject == null
                    ? EventBuilder.noopPublisher()
                    : pubsubEventBuilder.newPublisher(aquaProject, new AquaEvent.EventDescriptor());
            var api = HmfApi.create(apiUrl);
            subscriber.subscribe(new SnpCheck(api.runs(),
                    api.samples(),
                    StorageOptions.getDefaultInstance().getService(),
                    snpcheckBucketName,
                    new PerlVcfComparison(),
                    turquoisePublisher,
                    aquaPublisher,
                    publisher,
                    passthru,
                    alwaysPass));
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

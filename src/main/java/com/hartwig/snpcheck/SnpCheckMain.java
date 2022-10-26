package com.hartwig.snpcheck;

import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.ProjectTopicName;
import com.hartwig.api.HmfApi;
import com.hartwig.events.EventSubscriber;
import com.hartwig.events.PipelineComplete;
import com.hartwig.events.PipelineValidated;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

public class SnpCheckMain implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheckMain.class);

    @CommandLine.Option(names = { "--api_url" },
                        required = true,
                        description = "URL from which to connect to the API")
    private String apiUrl;

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
            description = "Mark all events as validated without actually validating against the snpcheck vcf.")
    private boolean passthru;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new Jdk8Module());
    }

    @Override
    public Integer call() {
        try {
            HmfApi hmfApi = HmfApi.create(apiUrl);
            if (passthru && project.contains("prod")){
                LOGGER.error("Snpcheck does not allow configuring passthru on a production project.");
                return 1;
            }

            EventSubscriber.create(project,
                    PipelineComplete.subscription(project, "snpcheck"),
                    PipelineComplete.class)
                    .subscribe(new SnpCheck(hmfApi.runs(),
                            hmfApi.samples(),
                            StorageOptions.getDefaultInstance().getService(),
                            snpcheckBucketName,
                            new PerlVcfComparison(),
                            Publisher.newBuilder(ProjectTopicName.of(project, "turquoise.events")).build(),
                            Publisher.newBuilder(ProjectTopicName.of(project, PipelineValidated.TOPIC)).build(),
                            OBJECT_MAPPER, passthru));
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

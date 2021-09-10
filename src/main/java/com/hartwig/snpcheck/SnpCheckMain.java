package com.hartwig.snpcheck;

import java.io.FileInputStream;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.ProjectTopicName;
import com.hartwig.api.HmfApi;
import com.hartwig.events.EventSubscriber;
import com.hartwig.events.PipelineStaged;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

public class SnpCheckMain implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnpCheckMain.class);

    @CommandLine.Option(names = { "--snpcheck_private_key" },
                        defaultValue = "/snpcheck_secrets/service_account.json",
                        description = "Path to private key for the snpcheck service account")
    private String snpcheckPrivateKeyPath;
    @CommandLine.Option(names = { "--database_private_key" },
                        defaultValue = "/database_secrets/service_account.json",
                        description = "Path to private key for the database service account")
    private String databasePrivateKeyPath;
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
            GoogleCredentials snpCheckCredentials = GoogleCredentials.fromStream(new FileInputStream(snpcheckPrivateKeyPath));
            GoogleCredentials databaseCredentials = GoogleCredentials.fromStream(new FileInputStream(databasePrivateKeyPath));
            Bucket snpcheckBucket =
                    StorageOptions.newBuilder().setCredentials(snpCheckCredentials).build().getService().get(snpcheckBucketName);
            if (snpcheckBucket == null) {
                LOGGER.error("Bucket [{}] does not exist. ", snpcheckBucketName);
                return 1;
            }
            EventSubscriber.create(project, snpCheckCredentials,
                    PipelineStaged.subscription(project, "snpcheck", snpCheckCredentials), PipelineStaged.class)
                    .subscribe(new SnpCheck(hmfApi.runs(),
                            hmfApi.samples(),
                            snpcheckBucket,
                            StorageOptions.newBuilder().setCredentials(databaseCredentials).build().getService(),
                            new PerlVcfComparison(),
                            Publisher.newBuilder(ProjectTopicName.of(project, "turquoise.events"))
                                    .setCredentialsProvider(() -> snpCheckCredentials)
                                    .build(), OBJECT_MAPPER));
            return 0;
        } catch (Exception e) {
            LOGGER.error("Exception while running snpcheck", e);
            return 1;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SnpCheckMain()).execute(args);
        System.exit(exitCode);
    }
}

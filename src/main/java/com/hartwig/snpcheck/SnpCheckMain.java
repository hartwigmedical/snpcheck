package com.hartwig.snpcheck;

import java.io.FileInputStream;
import java.util.concurrent.Callable;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.ProjectTopicName;
import com.hartwig.api.HmfApi;

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
            Publisher publisher = Publisher.newBuilder(ProjectTopicName.of(project, "turquoise.events"))
                    .setCredentialsProvider(() -> snpCheckCredentials)
                    .build();
            new SnpCheck(hmfApi.runs(),
                    hmfApi.samples(),
                    snpcheckBucket,
                    StorageOptions.newBuilder().setCredentials(databaseCredentials).build().getService(),
                    new PerlVcfComparison(),
                    publisher).run();
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

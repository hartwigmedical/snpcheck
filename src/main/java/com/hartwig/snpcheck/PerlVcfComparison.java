package com.hartwig.snpcheck;

import static java.nio.file.Files.createFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.cloud.storage.Blob;
import com.hartwig.api.model.Run;

public class PerlVcfComparison implements VcfComparison {

    @Override
    public Result compare(final Run run, final Blob refVcf, final Blob valVcf, final Boolean alwaysPass) {
        Path workingDirectory = Paths.get(run.getId() + "-working");
        try {
            if (!Files.exists(workingDirectory)) {
                Files.createDirectory(workingDirectory);
            }
            Path valVcfLocal = workingDirectory.resolve("val.vcf");
            Path refVcfLocal = workingDirectory.resolve("ref.vcf");
            Files.deleteIfExists(valVcfLocal);
            Files.deleteIfExists(refVcfLocal);
            Files.write(createFile(valVcfLocal), valVcf.getContent());
            Files.write(createFile(refVcfLocal), refVcf.getContent());
            return new PerlVcfComparisonExecution().execute(refVcfLocal.toString(), valVcfLocal.toString(), alwaysPass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

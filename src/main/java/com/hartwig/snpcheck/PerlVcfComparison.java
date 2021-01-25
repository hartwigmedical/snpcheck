package com.hartwig.snpcheck;

import static java.nio.file.Files.createFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.cloud.storage.Blob;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerlVcfComparison implements VcfComparison {

    @Override
    public Result compare(final Run run, final Blob refVcf, final Blob valVcf) {
        Path workingDirectory = Paths.get(run.getId() + "-working");
        try {
            if (!Files.exists(workingDirectory)) {
                Files.createDirectory(workingDirectory);
            }
            Path valVcfLocal = Files.write(createFile(workingDirectory.resolve("val.vcf")), valVcf.getContent());
            Path refVcfLocal = Files.write(createFile(workingDirectory.resolve("ref.vcf")), refVcf.getContent());
            Process perlScriptExecution =
                    new ProcessBuilder().command("./snpcheck_compare_vcfs", refVcfLocal.toString(), valVcfLocal.toString())
                            .inheritIO()
                            .start();
            return perlScriptExecution.waitFor() == 0 ? Result.PASS : Result.FAIL;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

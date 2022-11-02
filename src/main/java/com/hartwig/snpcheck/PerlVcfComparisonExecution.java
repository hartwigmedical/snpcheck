package com.hartwig.snpcheck;

import com.hartwig.snpcheck.VcfComparison.Result;

import java.io.IOException;

public class PerlVcfComparisonExecution {

    private static final String COMPARE_SCRIPT = "./snpcheck_compare_vcfs";
    private final boolean alwaysPass;

    public PerlVcfComparisonExecution(final boolean alwaysPass) {
        this.alwaysPass = alwaysPass;
    }

    public Result execute(final String refVcf, final String valVcf) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            if (alwaysPass) {
                processBuilder.command(COMPARE_SCRIPT, "-alwaysPass", refVcf, valVcf);
            } else {
                processBuilder.command(COMPARE_SCRIPT, refVcf, valVcf);
            }
            Process perlScriptExecution = processBuilder.inheritIO().start();

            return perlScriptExecution.waitFor() == 0 ? Result.PASS : Result.FAIL;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

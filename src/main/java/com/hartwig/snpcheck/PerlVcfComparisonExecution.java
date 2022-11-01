package com.hartwig.snpcheck;

import java.io.IOException;
import com.hartwig.snpcheck.VcfComparison.Result;

public class PerlVcfComparisonExecution {

    private static final String COMPARE_SCRIPT = "./snpcheck_compare_vcfs";

    public Result execute(final String refVcf, final String valVcf, final Boolean alwaysPass) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            if (alwaysPass){
                processBuilder.command(COMPARE_SCRIPT, "-alwaysPass", refVcf, valVcf);
            }else{
                processBuilder.command(COMPARE_SCRIPT, refVcf, valVcf);
            }
            Process perlScriptExecution = processBuilder.inheritIO().start();

            return perlScriptExecution.waitFor() == 0 ? Result.PASS : Result.FAIL;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

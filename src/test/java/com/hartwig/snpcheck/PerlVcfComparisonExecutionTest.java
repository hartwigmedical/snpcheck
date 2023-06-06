package com.hartwig.snpcheck;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.snpcheck.VcfComparison.Result;
import org.junit.jupiter.api.Test;

public class PerlVcfComparisonExecutionTest {
    private static final String USER_DIR = System.getProperty("user.dir");
    private static final String BAD_GENOTYPE_VCF = USER_DIR + "/src/test/resources/bad_genotype.vcf";
    private static final String GOOD_GENOTYPE_VCF_1 = USER_DIR + "/src/test/resources/good1_genotype.vcf";
    private static final String GOOD_GENOTYPE_VCF_2 = USER_DIR + "/src/test/resources/good2_genotype.vcf";

    @Test
    public void goodSnpcheckYieldsPass() {
        Result result = new PerlVcfComparisonExecution().execute(GOOD_GENOTYPE_VCF_1, GOOD_GENOTYPE_VCF_2, false);
        assertThat(result).isEqualTo(Result.PASS);
    }

    @Test
    public void badSnpcheckYieldsFail() {
        Result result = new PerlVcfComparisonExecution().execute(GOOD_GENOTYPE_VCF_1, BAD_GENOTYPE_VCF, false);
        assertThat(result).isEqualTo(Result.FAIL);
    }

    @Test
    public void badSnpcheckAlwaysPassYieldsPass() {
        Result result = new PerlVcfComparisonExecution().execute(GOOD_GENOTYPE_VCF_1, BAD_GENOTYPE_VCF, true);
        assertThat(result).isEqualTo(Result.PASS);
    }
}

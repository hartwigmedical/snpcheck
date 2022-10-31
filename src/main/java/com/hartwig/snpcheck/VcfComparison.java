package com.hartwig.snpcheck;

import com.google.cloud.storage.Blob;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.Sample;

public interface VcfComparison {
    enum Result {
        PASS,
        FAIL
    }

    Result compare(final Run run, final Blob refVcf, final Blob valVcf, final Boolean alwaysPass);
}

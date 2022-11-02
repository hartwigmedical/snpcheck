package com.hartwig.snpcheck;

import com.google.cloud.storage.Blob;
import com.hartwig.api.model.Run;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class PerlVcfComparisonTest {

    @Test
    public void perlVcfComparisonIsPassedCorrectValAndRefVcfLocations() {
        PerlVcfComparisonExecution execution = mock(PerlVcfComparisonExecution.class);
        PerlVcfComparison victim = new PerlVcfComparison(execution);
        ArgumentCaptor<String> refVcfCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tumVcfCaptor = ArgumentCaptor.forClass(String.class);

        Blob refVcfBlob = addContent(mock(Blob.class));
        Blob tumVcfBlob = addContent(mock(Blob.class));
        victim.compare(new Run().id(1L), refVcfBlob, tumVcfBlob);

        verify(execution).execute(refVcfCaptor.capture(), tumVcfCaptor.capture());
        assertThat(refVcfCaptor.getValue()).isEqualTo("1-working/ref.vcf");
        assertThat(tumVcfCaptor.getValue()).isEqualTo("1-working/val.vcf");
    }

    private static Blob addContent(final Blob refVcfBlob) {
        when(refVcfBlob.getContent()).thenReturn("content".getBytes());
        return refVcfBlob;
    }

}
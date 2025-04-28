# Snpcheck
A post-pipeline validation which used to compare the results of the unifiedgenotyper against a control VCF as a final QC.

## Discontinued

The VCF comparison has be discontinued at 25-04-2025 in favor of an Amber-based check by the medical team.

The snpcheck service is still required in the current infrastructure because it acts as a filter for pipeline events.
It ensures that only somatic and single-sample pipeline runs without failures get status "validated" and are processed 
by downstream services.

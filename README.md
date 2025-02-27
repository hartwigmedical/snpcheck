# Snpcheck
A post-pipeline validation which compares the results of the unifiedgenotyper against a control VCF as a final QC.

#### Creating cloudbuild image

This project requires perl modules not in standard "maven image". The following code was used to create the cloudbuild image.

```
docker build . -t "europe-west4-docker.pkg.dev/hmf-build/hmf-docker/maven:3.6.0-jdk-11-slim-libarray-diff-perl"
docker push "europe-west4-docker.pkg.dev/hmf-build/hmf-docker/maven:3.6.0-jdk-11-slim-libarray-diff-perl"
```

With the following Dockerfile contents:
```
FROM --platform=linux/amd64 maven:3.6.0-jdk-11-slim

RUN apt-get update && \
    apt-get install -y libarray-diff-perl
```
The `--platform` parameter is only required for building on non-amd devices (eg M1 Mac).

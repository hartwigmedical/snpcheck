FROM debian:stable

RUN apt-get update && \
    apt-get install -y curl httpie jq libarray-diff-perl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    curl https://raw.githubusercontent.com/hartwigmedical/qc-checks/v1.4/pipeline/snpcheck_compare_vcfs -o /usr/local/bin/snpcheck_compare_vcfs && \
    chmod +x /usr/local/bin/snpcheck_compare_vcfs && \
    rm -rf /var/lib/apt/lists/*

COPY run.sh /usr/local/bin/run.sh

CMD ["run.sh"]

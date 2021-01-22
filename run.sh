#!/bin/bash

function publishToTurquoise() {
    status=$1
    sample=$2
    timestamp="$(date -u "+%Y-%m-%dT%H:%M:%SZ[UTC]")"
    event=$(cat event.json | sed -e "s/STATUS/$status/g" -e "s/SAMPLE/$sample/g" -e "s/TIMESTAMP/$timestamp/g")
    gcloud pubsub topics publish turquoise.events --message "$event" --project hmf-pipeline-prod-e45b00f2
}

function checkRuns() {
    queue=$1
    SIZE=$(jq length $queue)

    echo "- Found $SIZE possible runs to SnpCheck"

    if [ $SIZE -gt 0 ];
    then
      # Import credentials
      gcloud auth activate-service-account --key-file=/key_snpcheck/service_account.json
      gcloud auth activate-service-account --key-file=/key_download/service_account.json

      # Get SnpCheck files
      gcloud config set account hmf-snpcheck@hmf-pipeline-prod-e45b00f2.iam.gserviceaccount.com
      gsutil ls gs://hmf-snpcheck/snpcheckvcfs/** > snp

      if [ $? -ne 0 ];
      then
        echo "- GS ls snpcheckvcfs failed"
        cat snp
        exit 1
      fi

      for ID in $(seq 1 $SIZE);
      do
        ID=$((ID-1))
        SET_NAME=$(jq -r .[$ID].set.name $queue)
        RUN_ID=$(jq -r .[$ID].id $queue)
        BUCKET=$(jq -r .[$ID].bucket $queue)
        REF_SAMPLE=$(jq -r .[$ID].set.ref_sample $queue)
        TUMOR_SAMPLE=$(jq -r .[$ID].set.tumor_sample $queue)

        # NOTE: This is assuming that the third part of the set name is the reference barcode. This is to support non FR barcodes, ideally this should actually make an API call that follows the run to the set and then directly loads the reference sample, but when I was writing this I didn't have the time to actually do that and we were on a bit of busy work. If HMF ever create a set without the barcode in the third slot this will break!
        REF_BARCODE=$(echo ${SET_NAME} | awk -F _ '{print $3}')

        echo ""
        echo "==== FOUND SNPCHECK PENDING RUN ===="
        echo "Run ID: ${RUN_ID}"
        echo "Set name: ${SET_NAME}"
        echo "Bucket: ${BUCKET}"
        echo "Ref Sample: ${REF_SAMPLE}"
        echo "Ref Barcode: ${REF_BARCODE}"
        echo "===================================="

        VCF_PATH=$(grep "${REF_BARCODE}-" snp)

        if [ $? -eq 0 ];
        then
          VCF_PATH=$(echo "$VCF_PATH" | tail -n 1)

          mkdir workdir

          gcloud config set account hmf-snpcheck@hmf-pipeline-prod-e45b00f2.iam.gserviceaccount.com
          gsutil cp "${VCF_PATH}" workdir/val.vcf

          if [ $? -eq 0 ];
          then
            REF="gs://${BUCKET}/${SET_NAME}/${REF_SAMPLE}/snp_genotype/snp_genotype_output.vcf"

            gcloud config set account hmf-download@hmf-database.iam.gserviceaccount.com
            gsutil -u hmf-database cp "$REF" workdir/ref.vcf

            if [ -f "workdir/ref.vcf" ];
            then
              echo ""
              echo " About to run snpcheck_compare_vcfs for"
              echo "- Ref: ${REF}" > workdir/output
              echo "- Val: ${VCF_PATH}" >> workdir/output
              echo "" >> workdir/output

              snpcheck_compare_vcfs workdir/ref.vcf workdir/val.vcf >> workdir/output 2>&1
              if [ $? -eq 0 ];
              then
                echo "" >> workdir/output
                echo "- Detected success" >> workdir/output
                STATUS=Validated
              else
                echo "" >> workdir/output
                echo "- Detected failure" >> workdir/output
                STATUS=Failed
              fi

              cat workdir/output

              gcloud config set account hmf-snpcheck@hmf-pipeline-prod-e45b00f2.iam.gserviceaccount.com
              gsutil cp workdir/output "gs://hmf-snpcheck/snpchecklogs/${SET_NAME}-$(date +%Y%m%d_%H%M%S).txt"


              if [ $? -ne 0 ];
              then
                echo "- Worklog upload failure"
                exit
              else
                curl -d '{"status": "'$STATUS'"}' -H "Content-Type: application/json" -X PATCH http://hmfapi/hmf/v1/runs/${RUN_ID}
                if [ $STATUS = "Failed" ];then
                  curl -d '{"failure": { "type": "QCFailure", "source": "SnpCheck"}}' -H "Content-Type: application/json" -X PATCH http://hmfapi/hmf/v1/runs/${RUN_ID}
                fi
                publishToTurquoise ${STATUS} ${TUMOR_SAMPLE}
              fi
            else
              echo "- REF vcf download failed (gs://${BUCKET}/${SET_NAME}/${REF_SAMPLE})"
            fi
          else
            echo "- VAL vcf download failed (${VCF_PATH})"
          fi

          rm -rf workdir

        else
          echo "- No SNP vcf found (by matching with ${REF_BARCODE}-)"
        fi
      done
    fi

}

echo ""
echo ""

while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    --testmode)
    TESTMODE=true
    ;;
    *)
            # unknown option
    ;;
  esac
  shift # past argument or value
done

# Get SnpCheck runs
http GET "hmfapi/hmf/v1/runs?status=Finished&ini=Somatic.ini" > somatic_queue
http GET "hmfapi/hmf/v1/runs?status=Finished&ini=Single.ini" > single_queue

if [ $? -ne 0 ];
then
  echo "- Failed to reach the API"
  exit
fi

checkRuns somatic_queue
checkRuns single_queue



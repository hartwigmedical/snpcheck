#!/usr/bin/env bash

/usr/bin/java ${JAVA_OPTS} -jar /usr/share/hartwig/snpcheck.jar "$@"
status=$?
if [ ${status} -ne 0 ]; then
  echo "Failed to start snpcheck: $status"
  exit ${status}
fi

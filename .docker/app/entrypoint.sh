#!/bin/bash
# Custom entrypoint that writes the GCE_KEY to file.
# The official entrypoint is located here: https://github.com/PrefectHQ/prefect/blob/master/entrypoint.sh

# $GCE_KEY contains credentials in JSON format. If it is missing or empty, show a warning.
# Otherwise write it to the path stored in $GOOGLE_APPLICATION_CREDENTIALS.
if [[ -z "${GCE_KEY}" ]]; then
  echo "WARNING: the \$GCE_KEY environment variable is not provided, so flows will not be able to connect to GCP."
else
  echo "Writing \$GCE_KEY to ${GOOGLE_APPLICATION_CREDENTIALS}..."
  # printf incantation is suggested in https://stackoverflow.com/a/49418406/331030 to correctly write special characters
  printf "%s" "${GCE_KEY}" > "${GOOGLE_APPLICATION_CREDENTIALS}"
fi

exec "$@"

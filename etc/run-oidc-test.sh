#!/usr/bin/env bash
# run-oidc-test
# Runs oidc auth tests.
set -eu

echo "Running MONGODB-OIDC authentication tests"

OIDC_ENV="${OIDC_ENV:-"test"}"

if [ $OIDC_ENV == "test" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi
    source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh

elif [ $OIDC_ENV == "azure" ]; then
    source ./env.sh

elif [ $OIDC_ENV == "gcp" ]; then
    source ./secrets-export.sh

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"

make -s evg-test-oidc-auth

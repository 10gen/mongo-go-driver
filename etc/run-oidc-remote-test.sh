#!/usr/bin/env bash
# run-oidc-test
# Runs oidc auth tests.
set -eu

echo "Running remote MONGODB-OIDC authentication tests on $OIDC_ENV"

DRIVERS_TAR_FILE=/tmp/mongo-go-driver.tar.gz
# we need to statically link libc to avoid the situation where the VM has a different
# version of libc
go build -tags osusergo,netgo -ldflags '-w -extldflags "-static -lgcc -lc"' -o test ./internal/cmd/testoidcauth/main.go
rm "$DRIVERS_TAR_FILE" || true
tar -cf $DRIVERS_TAR_FILE ./test
tar -uf $DRIVERS_TAR_FILE ./etc
rm "$DRIVERS_TAR_FILE".gz || true
gzip $DRIVERS_TAR_FILE

if [ $OIDC_ENV == "azure" ]; then
    export AZUREOIDC_DRIVERS_TAR_FILE=$DRIVERS_TAR_FILE
    # Define the command to run on the azure VM.
    # Ensure that we source the environment file created for us, set up any other variables we need,
    # and then run our test suite on the vm.
    export AZUREOIDC_TEST_CMD="PROJECT_DIRECTORY='.' OIDC_ENV=azure OIDC=oidc ./etc/run-oidc-test.sh ./test"
    bash ${DRIVERS_TOOLS}/.evergreen/auth_oidc/azure/run-driver-test.sh

elif [ $OIDC_ENV == "gcp" ]; then
    export GCPOIDC_DRIVERS_TAR_FILE=$DRIVERS_TAR_FILE
    # Define the command to run on the gcp VM.
    # Ensure that we source the environment file created for us, set up any other variables we need,
    # and then run our test suite on the vm.
    export GCPOIDC_TEST_CMD="PROJECT_DIRECTORY='.' OIDC_ENV=gcp OIDC=oidc ./etc/run-oidc-test.sh ./test"
    bash ${DRIVERS_TOOLS}/.evergreen/auth_oidc/gcp/run-driver-test.sh

elif [ $OIDC_ENV == "k8s" ]; then
    export K8S_VARIANT=${VARIANT}
    export K8S_DRIVERS_TAR_FILE=$DRIVERS_TAR_FILE
    export K8S_TEST_CMD="PROJECT_DIRECTORY='.' OIDC_ENV=k8s OIDC=oidc ./etc/run-oidc-test.sh ./test"
    bash ${DRIVERS_TOOLS}/.evergreen/auth_oidc/k8s/setup-pod.sh
    bash ${DRIVERS_TOOLS}/.evergreen/auth_oidc/k8s/run-driver-test.sh
    bash ${DRIVERS_TOOLS}/.evergreen/auth_oidc/k8s/teardown-pod.sh

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

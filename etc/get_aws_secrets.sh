#!/usr/bin/env bash
# get-aws-secrets
# Gets AWS secrets from the vault
set -eux

if [ -z "$DRIVERS_TOOLS" ]; then
    echo "Please define DRIVERS_TOOLS variable"
    exit 1
fi

# TODO: this should be built into activate-authawsvenv.sh
export PIP_QUIET=1
pushd ${DRIVERS_TOOLS}/.evergreen/auth_aws
. ./activate-authawsvenv.sh
# TODO: this should be built into activate-authawsvenv.sh
pip install pyyaml
# TODO: make the python3 finder less verbose
popd

# TODO: add note in setup_secrets.py about setup and using
# AWS_PROFILE
# TODO: Add a section to https://wiki.corp.mongodb.com/display/DRIVERS/Using+AWS+Secrets+Manager+to+Store+Testing+Secrets
# about using a bash script that can be run locally
python ${DRIVERS_TOOLS}/.evergreen/auth_aws/setup_secrets.py $@

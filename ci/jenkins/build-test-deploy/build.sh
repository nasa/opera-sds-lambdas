#!/bin/bash
set -ex

#Get the tag from the end of the GIT_BRANCH
BRANCH="${GIT_BRANCH##*/}"

#Get repo path by removing http://*/ and .git from GIT_URL
REPO="${GIT_URL#*://*/}"
REPO="${REPO%.git}"
#REPO="${REPO//\//_}"

echo "BRANCH: $BRANCH"
echo "REPO: $REPO"
echo "WORKSPACE: $WORKSPACE"
echo "GIT_OAUTH_TOKEN: $GIT_OAUTH_TOKEN"

#Get tag
TAG=$BRANCH
echo "TAG: $TAG"

source /home/hysdsops/verdi/bin/activate

rm -rf ${WORKSPACE}/lambda_packages

pushd ${WORKSPACE}/lambdas/cnm_r
python setup.py package --version ${TAG} --workspace workspace --lambda-func lambda_function-cnm_response.py --package-dir ${WORKSPACE}/lambda_packages
popd
pushd ${WORKSPACE}/lambdas/harikiri
python setup.py package --version ${TAG} --workspace workspace --lambda-func harikiri.py --package-dir ${WORKSPACE}/lambda_packages
popd
pushd ${WORKSPACE}/lambdas/isl
python setup.py package --version ${TAG} --workspace workspace --lambda-func isl.py --package-dir ${WORKSPACE}/lambda_packages
popd
pushd ${WORKSPACE}/lambdas/isl-sns
python setup.py package --version ${TAG} --workspace workspace --lambda-func isl-sns.py --package-dir ${WORKSPACE}/lambda_packages
popd
pushd ${WORKSPACE}/lambdas/event-misfire
python setup.py package --version ${TAG} --workspace workspace --lambda-func event-misfire.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/timer
python setup.py package --version ${TAG} --workspace workspace --lambda-func timer_handler.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/report
python setup.py package --version ${TAG} --workspace workspace --lambda-func report_handler.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/data-subscriber-download
python setup.py package --version ${TAG} --workspace workspace --lambda-func data_subscriber_download_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/data-subscriber-download-slc-ionosphere
python setup.py package --version ${TAG} --workspace workspace --lambda-func data_subscriber_download_slc_ionosphere_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/data-subscriber-query
python setup.py package --version ${TAG} --workspace workspace --lambda-func data_subscriber_query_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/batch_process
python setup.py package --version ${TAG} --workspace workspace --lambda-func batch_process_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd
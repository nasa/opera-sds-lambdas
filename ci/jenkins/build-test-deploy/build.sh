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

# activate python3.12 venv
source /home/hysdsops/verdi-py3.12/bin/activate

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
# Copy datetime_utils.py if it's a symlink (resolve it for packaging)
if [ -L datetime_utils.py ]; then
    cp -L datetime_utils.py datetime_utils.py.tmp
    mv datetime_utils.py.tmp datetime_utils.py
fi
python setup.py package --version ${TAG} --workspace workspace --lambda-func event-misfire.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/timer
python setup.py package --version ${TAG} --workspace workspace --lambda-func timer_handler.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/report
# Copy datetime_utils.py if it's a symlink (resolve it for packaging)
if [ -L datetime_utils.py ]; then
    cp -L datetime_utils.py datetime_utils.py.tmp
    mv datetime_utils.py.tmp datetime_utils.py
fi
python setup.py package --version ${TAG} --workspace workspace --lambda-func report_handler.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/data-subscriber-download
# Copy datetime_utils.py if it's a symlink (resolve it for packaging)
if [ -L datetime_utils.py ]; then
    cp -L datetime_utils.py datetime_utils.py.tmp
    mv datetime_utils.py.tmp datetime_utils.py
fi
python setup.py package --version ${TAG} --workspace workspace --lambda-func data_subscriber_download_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/data-subscriber-download-slc-ionosphere
# Copy datetime_utils.py if it's a symlink (resolve it for packaging)
if [ -L datetime_utils.py ]; then
    cp -L datetime_utils.py datetime_utils.py.tmp
    mv datetime_utils.py.tmp datetime_utils.py
fi
python setup.py package --version ${TAG} --workspace workspace --lambda-func data_subscriber_download_slc_ionosphere_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/data-subscriber-query
# Copy datetime_utils.py if it's a symlink (resolve it for packaging)
if [ -L datetime_utils.py ]; then
    cp -L datetime_utils.py datetime_utils.py.tmp
    mv datetime_utils.py.tmp datetime_utils.py
fi
python setup.py package --version ${TAG} --workspace workspace --lambda-func data_subscriber_query_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

pushd ${WORKSPACE}/lambdas/batch_process
# Copy datetime_utils.py if it's a symlink (resolve it for packaging)
if [ -L datetime_utils.py ]; then
    cp -L datetime_utils.py datetime_utils.py.tmp
    mv datetime_utils.py.tmp datetime_utils.py
fi
python setup.py package --version ${TAG} --workspace workspace --lambda-func batch_process_lambda.py --package-dir ${WORKSPACE}/lambda_packages
popd

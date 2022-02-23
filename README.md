# opera-pcm-lambdas
This repo is intended for building and deploying Lambda packages for use with OPERA PCM

# Building the Package

Building the Lambda package is done through the Jenkins User Interface. It requires 
a set of parameters to build and deploy. Below is a description of what the job needs:

 
| Variable          | Description              | Default Value |
|:-----------------:|:------------------------:|:--------------|
| ART_URL | Artifactory base url | https://cae-artifactory.jpl.nasa.gov/artifactory |
| ART_PATH | Root Artifactory path of where to publish the artifacts. The job will append the release tag to the given ART_PATH value (i.e. general-develop/gov/nasa/jpkl/opera/sds/pcm/lambda/develop) | general-develop/gov/nasa/jpl/opera/sds/pcm/lambda/ |
| ART_CREDENTIALS | ID of credentials containing the Artifactory username and encrypted password that will allow for Artifactory uploads/downloads. This info can be obtained by going to the Artifactory site and looking at your user profile settings.  | |
| GIT_OAUTH_TOKEN | ID of the Github OAuth token | |

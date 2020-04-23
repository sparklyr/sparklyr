# Dockerfile to setup the Jenkins for sparklyr integration tests.
# For more details on installing Jenkins on the machine, follow the instruction from https://jenkins.io/doc/book/installing/.
# When creating the image, you need to build the container and run with
# sudo docker run --rm --detach --network jenkins --env DOCKER_HOST=tcp://docker:2376 --env DOCKER_CERT_PATH=/certs/client --env DOCKER_TLS_VERIFY=1 --publish 8080:8080 --publish 50000:50000 --volume jenkins-data:/var/jenkins_home  --volume jenkins-docker-certs:/certs/client:ro --name jenkins-blueocean

FROM jenkinsci/blueocean

USER root

RUN apk add --repository http://dl-cdn.alpinelinux.org/alpine/v3.10/main python3==3.7.5-r1 --update-cache

# intall databricks-cli
RUN pip3 install databricks-cli
#install db-connect
RUN pip3 install -U databricks-connect==6.4.1

# install R
RUN apk add --repository http://dl-cdn.alpinelinux.org/alpine/v3.10/community --repository http://dl-cdn.alpinelinux.org/alpine/v3.10/main R==3.6.0-r1 R-dev==3.6.0-r1 --update-cache
RUN apk add make build-base linux-headers libxml2 libxml2-dev
RUN R --slave --vanilla -e 'install.packages("testthat", repos="https://cran.microsoft.com/snapshot/2019-04-15/")'
RUN R --slave --vanilla -e 'install.packages("devtools", repos="https://cran.microsoft.com/snapshot/2019-04-15/")'
RUN chmod a+rw -R /usr/lib/R/library/	# this allows jenkins to install packages

# workaround for the issue of copying R.css.
# https://github.com/r-lib/devtools/issues/2084#issuecomment-529685532 
RUN mkdir -p /usr/share/doc/R/html
RUN chmod a+rw -R /usr/share/doc/
RUN cp /usr/lib/R/library/stats/html/R.css /usr/share/doc/R/html/



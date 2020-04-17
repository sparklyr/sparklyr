import groovy.json.JsonOutput

def bash(String cmd) { sh("#/usr/bin/env bash\nset -euo pipefail\n${cmd}") }

def sparkHome = "/usr/lib/python3.7/site-packages/pyspark"
def s3Path = 'logs/' + env.BUILD_TAG + '/' + UUID.randomUUID().toString() + '.txt'
def s3Url = 'https://sparklyr-jenkins.s3.amazonaws.com/' + s3Path

pipeline {
    agent any
    stages {
        stage("Create Databricks cluster") {
            steps {
                script {
                    def uuid = UUID.randomUUID().toString()
                    def clusterParams = [
                        cluster_name: "jenkins-${uuid}",
                        spark_version: "6.3.x-scala2.11",
                        node_type_id: "i3.xlarge",
                        num_workers: 1,
                        autotermination_minutes: 10,
                    ]
                    def clusterParamsJson = JsonOutput.toJson(clusterParams)

                    def createClusterCmd = """databricks clusters create --json '${clusterParamsJson}'"""
                    createClusterOutput = sh(script: createClusterCmd, returnStdout: true)
                    echo createClusterOutput

                    def createClusterJson = readJSON(text: createClusterOutput)
                    // No def makes it a global variable, accessible in other stages
                    clusterId = createClusterJson["cluster_id"]
                }
            }
        }
        stage("Set up Databricks Connect") {
            steps {
                script {
                    withCredentials([
                        string(credentialsId: 'databricks-connect-api-token', variable: 'API_TOKEN'),
                        string(credentialsId: 'databricks-workspace-url', variable: 'WORKSPACE_URL')
                    ]) {
                        def dbConnectParams = [
                            host: WORKSPACE_URL,
                            token: API_TOKEN,
                            cluster_id: clusterId,
                            port: "15001",
                        ]
                        def dbConnectParamsJson = JsonOutput.toJson(dbConnectParams)
                        bash "echo '$dbConnectParamsJson' > ~/.databricks-connect"

                        // Smoke test to check if databricks-connect is set up correctly
                        bash "SPARK_HOME=${sparkHome} databricks-connect test  2>&1 | tee log.txt"
                    }
                }
            }
        }
        stage("Prepare the test data") {
            steps {
                bash """dbfs mkdirs dbfs:/tmp/data 2>&1 | tee -a log.txt"""
                bash """dbfs cp -r --overwrite tests/testthat/data dbfs:/tmp/data 2>&1 | tee -a log.txt"""
                // Listing files to avoid S3 consistency issues 
                bash """dbfs ls dbfs:/tmp/data 2>&1 | tee -a log.txt"""
            }
        }
        stage("Run tests") {
            steps {
                bash """R --vanilla --slave -e 'devtools::install(".", dependencies=TRUE)' 2>&1 | tee -a log.txt"""
                bash """SPARK_VERSION=2.4.4 SPARK_HOME=${sparkHome} TEST_DATABRICKS_CONNECT=true R --vanilla --slave -e 'devtools::test(stop_on_failure = TRUE)'  2>&1 | tee -a log.txt"""
            }
        }
    }
    post {
        always {
            bash "databricks clusters delete --cluster-id ${clusterId} 2>&1 | tee -a log.txt "
            bash """dbfs rm -r dbfs:/tmp/data  2>&1 | tee -a log.txt"""
            s3Upload(file: 'log.txt', bucket:'sparklyr-jenkins', path: s3Path, contentType: 'text/plain; charset=utf-8')
            script {
                if (env.CHANGE_ID) {
                    def comment = pullRequest.comment('Databricks Connect tests succeeded. View logs [here](' + s3Url + ').')
                }
            }
        }
        failure {
            script {
                // CHANGE_ID is set only for pull requests, so it is safe to access the pullRequest global variable
                if (env.CHANGE_ID) {
                    def comment = pullRequest.comment('Databricks Connect tests failed. View logs [here](' + s3Url + ').')
                }
            }
        }
    }
}

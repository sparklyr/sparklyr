import groovy.json.JsonOutput

def bash(String cmd) { sh("#/usr/bin/env bash\nset -euo pipefail\n${cmd}") }

def sparkHome = "/usr/lib/python3.7/site-packages/pyspark"
def randomId = UUID.randomUUID().toString() 
def s3Path = "logs/${env.BUILD_TAG}/${randomId}.txt"
def dbfsPath = 'dbfs:/tmp/sparklyr/' + randomId
def s3Url = 'https://sparklyr-jenkins.s3.amazonaws.com/' + s3Path

pipeline {
    agent any
    stages {
        stage("Launching Databricks cluster") {
            steps {
                script {
                    def clusterParams = [
                        cluster_name: "jenkins-${randomId}",
                        spark_version: "6.5.x-scala2.11",
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
        stage("Test sparklyr in Databricks notebooks (not Databricks Connect)") {
            steps {
                script {
                    def repo = sh(script: "git remote get-url origin | cut -d/ -f 4,5", returnStdout: true).trim()
                    bash "echo ${repo} | tee -a log.txt"

                    def sha = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
                    bash "echo ${sha} | tee -a log.txt"

                    def output = sh(script: "databricks jobs run-now --job-id 1 --notebook-params '{\"Github repo\": \"${repo}\", \"Commit ref\": \"${sha}\"}'", returnStdout: true)
                    def runId = readJSON(text: output)["run_id"]

                    // This job takes 2-5 minutes on both successful and failed runs
                    def timeout = 600;
                    def sleepDuration = 15;
                    def jobState;
                    for(int seconds = 0; seconds < timeout; seconds += sleepDuration) {
                        def jobInfo = sh(script: "databricks runs get --run-id ${runId}", returnStdout: true)
                        jobState = readJSON(text: jobInfo)["state"]
                        bash "echo ${jobState} | tee -a log.txt"
                        if (jobState["life_cycle_state"] == "TERMINATED") {
                            break;
                        }
                        sleep(sleepDuration)
                    }

                    def notebookOutputRaw = sh(script: "databricks runs get-output --run-id ${runId}", returnStdout: true)
                    def notebookOutput = readJSON(text: notebookOutputRaw)["notebook_output"]["result"]

                    bash "databricks runs get-output --run-id ${runId} | tee -a log.txt"

                    if (jobState["result_state"] != "SUCCESS" || !notebookOutput.startsWith("NOTEBOOK TEST PASS")) {
                        // Fail this stage but continue running other stages
                        // https://stackoverflow.com/questions/45021622/how-to-continue-past-a-failing-stage-in-jenkins-declarative-pipeline-syntax
                        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                            bash "echo 'Stage failed' | tee -a log.txt"
                            sh "exit 1"
                        }
                    }
                }
            }
        }
        stage("Setting up Databricks Connect") {
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
                        bash "SPARK_HOME=${sparkHome} databricks-connect test  2>&1 | tee -a log.txt"
                    }
                }
            }
        }
        stage("Copying test data to DBFS") {
            steps {
                bash """dbfs mkdirs ${dbfsPath} 2>&1 | tee -a log.txt"""
                bash """dbfs cp -r --overwrite tests/testthat/data ${dbfsPath} 2>&1 | tee -a log.txt"""
                // Listing files to avoid S3 consistency issues 
                bash """dbfs ls ${dbfsPath} 2>&1 | tee -a log.txt"""
            }
        }
        stage("Running tests") {
            steps {
                bash """R --vanilla --slave -e 'devtools::install(".", dependencies=TRUE)' 2>&1 | tee -a log.txt"""
                bash """SPARK_VERSION=2.4.4 SPARK_HOME=${sparkHome} TEST_DATABRICKS_CONNECT=true DBFS_DATA_PATH=${dbfsPath} R --vanilla --slave -e 'devtools::test(stop_on_failure = TRUE)'  2>&1 | tee -a log.txt"""
            }
        }
    }
    post {
        always {
            bash "databricks clusters delete --cluster-id ${clusterId} 2>&1 | tee -a log.txt "
            bash """dbfs rm -r ${dbfsPath}  2>&1 | tee -a log.txt"""
            s3Upload(file: 'log.txt', bucket:'sparklyr-jenkins', path: s3Path, contentType: 'text/plain; charset=utf-8')
        }
        success {
            script {
                if (env.CHANGE_ID) {
                    pullRequest.comment('Databricks Connect tests succeeded. View logs [here](' + s3Url + ').')
                }
            }
        }
        failure {
            script {
                if (env.CHANGE_ID) {
                    pullRequest.comment('Databricks Connect tests failed. View logs [here](' + s3Url + ').')
                }
            }
        }
    }
}

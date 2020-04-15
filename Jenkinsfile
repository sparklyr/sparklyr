import groovy.json.JsonOutput

def sparkHome = "/usr/lib/python3.7/site-packages/pyspark"
def s3Path = 'logs/' + env.BUILD_TAG + '/' + UUID.randomUUID().toString() + '.txt'

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
                        sh "echo '$dbConnectParamsJson' > ~/.databricks-connect"

                        // Smoke test to check if databricks-connect is set up correctly
                        sh "SPARK_HOME=${sparkHome} databricks-connect test > log.txt 2>&1"
                    }
                }
            }
        }
        stage("Prepare the test data") {
            steps {
                sh """dbfs mkdirs dbfs:/tmp/data"""
                sh """dbfs cp -r --overwrite tests/testthat/data dbfs:/tmp/data"""
                sh """echo 'Copied test files to DBFS'"""
            }
        }
        stage("Run tests") {
            steps {
                 sh """echo 'Installing dependencies'"""
                 sh """R --vanilla --slave -e 'devtools::install(".", dependencies=TRUE)'"""
                 sh """echo 'Running Tests'"""
                 sh """SPARK_VERSION=2.4.4 SPARK_HOME=${sparkHome} TEST_DATABRICKS_CONNECT=true R --vanilla --slave -e 'devtools::test(stop_on_failure = TRUE)' >> log.txt 2>&1"""
            }
        }
    }
    post {
        always {
            sh "databricks clusters delete --cluster-id ${clusterId}"
            sh """dbfs rm -r dbfs:/tmp/data"""
            s3Upload(file: 'log.txt', bucket:'sparklyr-jenkins', path: s3Path)
            script {
                if (env.CHANGE_ID) {
                    def comment = pullRequest.comment('Databricks Connect tests succeeded.')
                }
            }

        }
        failure {
            script {
                // CHANGE_ID is set only for pull requests, so it is safe to access the pullRequest global variable
                if (env.CHANGE_ID) {
                    def comment = pullRequest.comment('Databricks Connect tests failed')
                }
            }
        }
    }
}

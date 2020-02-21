import groovy.json.JsonOutput

def sparkHome = "/usr/lib/python3.7/site-packages/pyspark"

pipeline {
    agent any
    stages {
        stage("Create Databricks cluster") {
            steps {
                script {
                    def uuid = UUID.randomUUID().toString()
                    def clusterParams = [
                        cluster_name: "jenkins-${uuid}",
                        spark_version: "6.2.x-scala2.11",
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
                        sh "SPARK_HOME=${sparkHome} databricks-connect test"
                    }
                }
            }
        }
        stage("Run tests") {
            steps {
                sh """R --vanilla --slave -e 'devtools::install(".", dependencies=TRUE)'"""
                sh """SPARK_HOME=${sparkHome} R --vanilla --slave -e 'devtools::test()'"""
            }
        }
    }
    post {
        always {
            sh "databricks clusters delete --cluster-id ${clusterId}"
        }
    }
}

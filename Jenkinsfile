import groovy.json.JsonOutput

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
                echo "TODO 1"
            }
        }
        stage("Run tests") {
            steps {
                echo "TODO 2"
            }
        }
        stage("Terminate Databricks cluster") {
            steps {
                script {
                    sh "databricks clusters delete --cluster-id ${clusterId}"
                }
            }
        }
    }
}

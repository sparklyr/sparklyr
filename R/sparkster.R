#' Start an EC2 Spark Cluster
#'
#' This function will start an EC2 cluster.
#'
#' @param access_key_id EC2 access key id. Create a new access key from https://console.aws.amazon.com/iam/home?#security_credential
#' @param secret_access_key EC2 secret access key.
#' @param version The Spark version to use.
#' @param instance_count The total number of EC2 instances to be provisioned.
#' @param pem_path Identity file for ssh connections.
#' @param cluster_name Name used to identify cluster.
#' @export
start_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  instance_count = 1,
  version = "1.6.1",
  cluster_name = "sparkster") {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(paste("launch", cluster_name),
                  sparkInfo$sparkVersionDir,
                  access_key_id,
                  secret_access_key,
                  instance_count,
                  version,
                  pem_path)
}

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

  command_params <- paste("--region=us-east-1 ",
                          "--instance-type=m3.medium ",
                          "--copy-aws-credentials ",
                          "-s ",
                          instance_count)

  run_ec2_command(command = paste("launch", cluster_name),
                  command_params = command_params,
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path)
}

stop_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.1",
  cluster_name = "sparkster") {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("stop", cluster_name),
                  command_params = "",
                  input = "y",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path)
}

destroy_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.1",
  cluster_name = "sparkster") {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("destroy", cluster_name),
                  command_params = "",
                  input = "y",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path)
}

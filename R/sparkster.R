#' Start a local Spark cluster
#'
#' This function will launch the local cluster.
#'
#' @param version The Spark version to use.
#' @export
start_local <- function(
  version = "1.6.0") {

  setup_local(version)

  sc <<- sparkR.init(master = "local")
  sqlContext <<- sparkRSQL.init(sc)
}

#' Stop a local Spark cluster
#'
#' This function will stop the local cluster.
#'
#' @param version The Spark version to use.
#' @export
stop_local <- function(
  version = "1.6.0") {

  setup_local(version)

  sparkR.stop()
}

#' Launch an EC2 Spark cluster
#'
#' This function will launch a new an EC2 cluster.
#'
#' @param access_key_id EC2 access key id. Create a new access key from https://console.aws.amazon.com/iam/home?#security_credential
#' @param secret_access_key EC2 secret access key.
#' @param pem_path Identity file for ssh connections.
#' @param instance_count The total number of EC2 instances to be provisioned.
#' @param version The Spark version to use.
#' @param cluster_name Name used to identify cluster.
#' @param instance_type Type of EC2 instance. Tested with 'm3.medium' and 'c3.4xlarge'.
#' @param region The EC2 region to host this cluster.
#' @param preview Print the EC2 command without executing anything?
#' @export
launch_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  instance_count = 1,
  version = "1.6.0",
  cluster_name = "sparkster",
  instance_type = "c3.4xlarge",
  region = "us-west-1",
  preview = FALSE) {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  command_params <- paste("--region=us-east-1 ",
                          paste("--instance-type=", instance_type, sep = ""),
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

start_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  instance_count = 1,
  version = "1.6.0",
  cluster_name = "sparkster",
  preview = FALSE) {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("launch", cluster_name),
                  command_params = "",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path,
                  preview = preview)
}

stop_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.0",
  cluster_name = "sparkster",
  preview = FALSE) {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("stop", cluster_name),
                  command_params = "",
                  input = "y",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path,
                  preview = preview)
}

destroy_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.0",
  cluster_name = "sparkster",
  preview = FALSE) {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("destroy", cluster_name),
                  command_params = "",
                  input = "y",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path,
                  preview = preview)
}

login_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.0",
  cluster_name = "sparkster",
  preview = FALSE) {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("login", cluster_name),
                  command_params = "",
                  input = "y",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path,
                  preview = preview)
}

master_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.0",
  cluster_name = "sparkster",
  preview = FALSE) {

  sparkInfo <- download_spark(version)

  validate_pem(pem_path);

  run_ec2_command(command = paste("get-master", cluster_name),
                  command_params = "",
                  input = "",
                  spark_dir = sparkInfo$sparkVersionDir,
                  access_key_id = access_key_id,
                  secret_access_key = secret_access_key,
                  version = version,
                  pem_path = pem_path,
                  preview = preview)$stdout[[3]]
}

setup_ec2 <- function(
  access_key_id,
  secret_access_key,
  pem_path,
  version = "1.6.0",
  cluster_name = "sparkster") {

  sparkInfo <- download_spark(version)

  sparkHome <- sparkInfo$sparkVersionDir
  Sys.setenv(SPARK_HOME = sparkHome)
  .libPaths(c(file.path(sparkHome, "R", "lib")))

  library(SparkR)

  master <- master_ec2(access_key_id = access_key_id,
                       secret_access_key = secret_access_key,
                       pem_path = pem_path,
                       version = version,
                       cluster_name = cluster_name)

  sc <<- sparkR.init(master = master)
  sqlContext <<- sparkRSQL.init(sc)
}


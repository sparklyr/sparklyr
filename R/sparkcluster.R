#' Start an EC2 Spark Cluster
#'
#' This function will start an EC2 cluster.
#'
#' @param version The Spark version to use.
#' @param instance_count The total number of EC2 instances to be provisioned.
#'
#' @export
start_ec2 <- function(instance_count = 1, version = "1.6.1") {
  sparkDir <- download_spark(version)
  sparkDir
}

#' @param accessKeyId EC2 access key id. Create a new access key from https://console.aws.amazon.com/iam/home?#security_credential
#' @param secretAccessKey EC2 secret access key.
#' @param pemPath Identity file for ssh connections.
#' @param instanceCount The total number of EC2 instances to be provisioned.
#' @param version The Spark version to use.
#' @param clusterName Name used to identify cluster.
#' @param instanceType Type of EC2 instance. Tested with "m3.medium" and "c3.4xlarge".
#' @param region The EC2 region to host this cluster.
#' @param clusterInfo A collection of parameters required to use the EC2 cluster, initialized with spark_ec2_cluster.
#' @name ec2-spark
NULL

#' @rdname ec2-spark
#' @export
spark_ec2_cluster <- function(
  accessKeyId,
  secretAccessKey,
  pemPath,
  version = "1.6.0",
  clusterName = "spark",
  instanceType = "m3.medium",
  region = "us-east-1"
) {
  sparkInfo <- spark_check_install(version)
  validate_pem(pemPath);

  list(
    accessKeyId = accessKeyId,
    secretAccessKey = secretAccessKey,
    pemPath = pemPath,
    sparkDir = sparkInfo$sparkVersionDir,
    version = version,
    clusterName = clusterName,
    instanceType = instanceType,
    region = region
  )
}

#' Install an EC2 Spark cluster
#'
#' This function will install and launch a new an EC2 cluster and download required client components.
#' Returns a cluster information list to enable further commands.
#'
#' @rdname ec2-spark
#' @export
spark_ec2_deploy <- function(
  clusterInfo,
  instanceCount = 1,
  copyDir = NULL) {
  spark_check_install(clusterInfo$version)

  commandParams <- paste(paste("--region=", clusterInfo$region, sep = ""),
                         paste("--instance-type=", clusterInfo$instanceType, sep = ""),
                         "--copy-aws-credentials ",
                         "-s ",
                         instanceCount)

  if (!identical(copyDir, NULL)) {
    commandParams <- paste(commandParams,
                           "--deploy-root-dir",
                           copyDir)
  }

  command <- run_ec2_command(command = paste("launch", clusterInfo$clusterName),
                             commandParams = commandParams,
                             clusterInfo = clusterInfo,
                             parse = FALSE)

  command
}

#' Starts a previously stopped Spark instance in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_start <- function(
  clusterInfo,
  instanceCount = 1) {

  run_ec2_command(command = paste("launch", clusterInfo$clusterName),
                  commandParams = "",
                  clusterInfo = clusterInfo,
                  parse = FALSE)
}

#' Stops a running Spark instance in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_stop <- function(
  clusterInfo) {

  run_ec2_command(command = paste("stop", clusterInfo$clusterName),
                  commandParams = "",
                  input = "y",
                  clusterInfo =  clusterInfo,
                  parse = FALSE)
}

#' Deletes an Spark instance in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_destroy <- function(
  clusterInfo) {

  run_ec2_command(command = paste("destroy", clusterInfo$clusterName),
                  commandParams = "",
                  input = "y",
                  clusterInfo = clusterInfo,
                  parse = FALSE)
}

#' Logins into Spark in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_login <- function(
  clusterInfo) {

  res <- run_ec2_command(command = paste("login", clusterInfo$clusterName),
                         commandParams = "",
                         input = "y",
                         clusterInfo = clusterInfo,
                         preview = TRUE)

  cat(res$command)
}

#' Retrieves master location from EC2
#' @rdname ec2-spark
#' @export
spark_ec2_master <- function(
  clusterInfo) {
  validate_pem(clusterInfo$pemPath);

  run_ec2_command(command = paste("get-master", clusterInfo$clusterName),
                  commandParams = "",
                  input = "",
                  clusterInfo = clusterInfo)$stdout[[3]]
}

run_ec2_command <- function(command,
                            commandParams,
                            input = "",
                            clusterInfo = clusterInfo,
                            preview = FALSE,
                            parse = TRUE) {

  variables <- paste("AWS_ACCESS_KEY_ID=",
                     clusterInfo$accessKeyId,
                     " ",
                     "AWS_SECRET_ACCESS_KEY=",
                     clusterInfo$secretAccessKey,
                     sep = "")

  pemPath <- path.expand(clusterInfo$pemPath)
  pemName <- remove_extension(basename(clusterInfo$pemPath))
  params <- paste("--key-pair=",
                  pemName,
                  " ",
                  "--identity-file=",
                  pemPath,
                  " ",
                  commandParams,
                  sep = "")

  sparkEC2 <- file.path(clusterInfo$sparkDir, "ec2/spark-ec2")

  command <- paste(variables, sparkEC2, params, command)

  retval <- list(
    command = command
  )

  if (!preview) {
    stdoutFile <- tempfile(fileext="out")
    stderrFile <- tempfile(fileext="err")

    on.exit(unlink(stdoutFile))
    on.exit(unlink(stderrFile))

    if (parse) {
      system(paste(command, params, ">", stdoutFile, "2>", stderrFile), input = input)

      retval$stdout <- readLines(stdoutFile)
      retval$stderr <- readLines(stderrFile)

      retval
    }
    else {
      system(paste(command, params), input = input)
    }
  }
  else {
    retval
  }
}

#' Opens RStudio in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_rstudio <- function(
  clusterInfo) {
  master <- spark_ec2_master(clusterInfo)
  browseURL(paste("http://", master, ":8787", sep = ""))
}

#' Opens the Spark web interface in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_web <- function(
  clusterInfo) {
  master <- spark_ec2_master(clusterInfo)
  browseURL(paste("http://", master, "8080", sep = ""))
}

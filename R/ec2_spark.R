#' @param access_key_id EC2 access key id. Create a new access key from https://console.aws.amazon.com/iam/home?#security_credential
#' @param secret_access_key EC2 secret access key.
#' @param pem_file Identity file for ssh connections.
#' @param instance_count The total number of EC2 instances to be provisioned.
#' @param version The Spark version to use.
#' @param cluster_name Name used to identify cluster.
#' @param instance_type Type of EC2 instance. Tested with "m3.medium" and "c3.4xlarge".
#' @param region The EC2 region to host this cluster.
#' @param cluster_info A collection of parameters required to use the EC2 cluster, initialized with spark_ec2_cluster.
#' @param copy_dir Copies all the contents (recursevely) of the given path into the driver node durint spark_ec2_deploy
#' @name ec2-spark
NULL

#' @rdname ec2-spark
#' @export
spark_ec2_cluster <- function(
  access_key_id,
  secret_access_key,
  pem_file,
  version = "1.6.0",
  hadoop_version = "2.6",
  cluster_name = "spark",
  instance_type = "m3.medium",
  region = NULL
) {
  installInfo <- spark_install_find(version, hadoop_version)
  validate_pem(pem_file);

  list(
    accessKeyId = access_key_id,
    secretAccessKey = secret_access_key,
    pemFile = pem_file,
    sparkDir = installInfo$sparkVersionDir,
    version = version,
    hadoopVersion = hadoop_version,
    clusterName = cluster_name,
    instanceType = instance_type,
    region = region,
    installInfo = installInfo
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
  cluster_info,
  instance_count = 1,
  copy_dir = NULL) {

  commandParams <- ""
  if (!is.null(cluster_info$region)) {
    commandParams <- paste(commandParams,
                           paste("--region", cluster_info$region, sep = "="))
  }

  commandParams <- ""
  if (!is.null(cluster_info$instanceType)) {
    commandParams <- paste(commandParams,
                           paste("--instance-type", cluster_info$instanceType, sep = "="))
  }

  commandParams <- paste(commandParams,
                         "--copy-aws-credentials",
                         "-s",
                         instance_count)

  if (!identical(copy_dir, NULL)) {
    commandParams <- paste(commandParams,
                           "--deploy-root-dir",
                           copy_dir)
  }

  command <- run_ec2_command(command = paste("launch", cluster_info$clusterName),
                             commandParams = commandParams,
                             clusterInfo = cluster_info,
                             parse = FALSE)

  command
}

#' Starts a previously stopped Spark instance in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_start <- function(
  cluster_info,
  instance_count = 1) {

  run_ec2_command(command = paste("launch", cluster_info$clusterName),
                  commandParams = "",
                  clusterInfo = cluster_info,
                  parse = FALSE)
}

#' Stops a running Spark instance in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_stop <- function(
  cluster_info) {

  run_ec2_command(command = paste("stop", cluster_info$clusterName),
                  commandParams = "",
                  input = "y",
                  clusterInfo =  cluster_info,
                  parse = FALSE)
}

#' Deletes an Spark instance in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_destroy <- function(
  cluster_info) {

  run_ec2_command(command = paste("destroy", cluster_info$clusterName),
                  commandParams = "",
                  input = "y",
                  clusterInfo = cluster_info,
                  parse = FALSE)
}

#' Logins into Spark in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_login <- function(
  cluster_info) {

  res <- run_ec2_command(command = paste("login", cluster_info$clusterName),
                         commandParams = "",
                         input = "y",
                         clusterInfo = cluster_info,
                         preview = TRUE)

  cat(res$command)
}

#' Retrieves master location from EC2
#' @rdname ec2-spark
#' @export
spark_ec2_master <- function(
  cluster_info) {
  validate_pem(cluster_info$pemFile);

  run_ec2_command(command = paste("get-master", cluster_info$clusterName),
                  commandParams = "",
                  input = "",
                  clusterInfo = cluster_info)$stdout[[3]]
}

run_ec2_command <- function(command,
                            commandParams,
                            input = "",
                            clusterInfo = cluster_info,
                            preview = FALSE,
                            parse = TRUE) {

  variables <- paste("AWS_ACCESS_KEY_ID=",
                     clusterInfo$accessKeyId,
                     " ",
                     "AWS_SECRET_ACCESS_KEY=",
                     clusterInfo$secretAccessKey,
                     sep = "")

  pemFile <- path.expand(clusterInfo$pemFile)
  pemName <- remove_extension(basename(clusterInfo$pemFile))
  params <- paste("--key-pair=",
                  pemName,
                  " ",
                  "--identity-file=",
                  pemFile,
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
  cluster_info) {
  master <- spark_ec2_master(cluster_info)
  utils::browseURL(paste("http://", master, ":8787", sep = ""))
}

#' Opens the Spark web interface in EC2
#' @rdname ec2-spark
#' @export
spark_ec2_web <- function(
  cluster_info) {
  master <- spark_ec2_master(cluster_info)
  utils::browseURL(paste("http://", master, "8080", sep = ""))
}

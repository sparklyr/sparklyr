run_ec2_command <- function(command,
                            command_params,
                            input = "",
                            spark_dir,
                            access_key_id,
                            secret_access_key,
                            version,
                            pem_path,
                            preview = FALSE) {

  variables <- paste("AWS_ACCESS_KEY_ID=",
                     access_key_id,
                     " ",
                     "AWS_SECRET_ACCESS_KEY=",
                     secret_access_key,
                     sep = "")

  pem_path <- path.expand(pem_path)
  pem_name <- remove_extension(basename(pem_path))
  params <- paste("--key-pair=",
                  pem_name,
                  " ",
                  "--identity-file=",
                  pem_path,
                  " ",
                  command_params,
                  sep = "")

  sparkEC2 <- file.path(spark_dir, "ec2/spark-ec2")

  command <- paste(variables, sparkEC2, params, command)

  retval <- list(
    command = command
  )

  if (!preview) {
    stdoutFile <- tempfile(fileext="out")
    stderrFile <- tempfile(fileext="err")

    on.exit(unlink(stdoutFile))
    on.exit(unlink(stderrFile))

    system(paste(command, params, ">", stdoutFile, "2>", stderrFile), input = input)

    retval$stdout <- readLines(stdoutFile)
    retval$stderr <- readLines(stderrFile)
  }

  return(retval)
}

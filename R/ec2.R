run_ec2_command <- function(command,
                            sparkDir,
                            access_key_id,
                            secret_access_key,
                            instance_count,
                            version,
                            pem_path) {

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
                  "--region=us-east-1 ",
                  "--instance-type=m3.medium ",
                  "--copy-aws-credentials ",
                  "-s ",
                  instance_count,
                  sep = "")

  sparkEC2 <- file.path(sparkDir, "ec2/spark-ec2")
  system(paste(variables, sparkEC2, params, command))
}

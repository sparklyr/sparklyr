wait_file_exists <- function(filename, retries = 50) {
  while(!file.exists(filename) && retries >= 0) {
    retries <- retries  - 1;
    Sys.sleep(0.1)
  }

  file.exists(filename)

download_spark <- function(version) {
  componentName <- paste("spark-", version, "-bin-hadoop2.6", sep = "")

  packageName <- paste(componentName, ".tgz", sep = "")
  packageSource <- "http://d3kbcqa49mib13.cloudfront.net"

  if (!dir.exists("spark")) {
    print("Local spark directory for this project not found, creating.")
    dir.create("spark")
  }

  sparkDir <- file.path(getwd(), "spark")
  packagePath <- file.path(sparkDir, packageName)

  if (!file.exists(packagePath)) {
    print("Spark package not found, downloading.")
    download.file(file.path(packageSource, packageName), destfile = packagePath)
  }

  sparkVersionDir <- file.path(sparkDir, componentName)

  if (!dir.exists(sparkVersionDir)) {
    untar(tarfile = packagePath, exdir = sparkDir)
  }

  list (
    sparkDir = sparkDir,
    sparkVersionDir = sparkVersionDir
  )
}

remove_extension <- function(file) {
  sub("[.][^.]*$", "", file, perl=TRUE)
}

validate_pem <- function(pem_path) {
  if (!file.exists(pem_path)) {
    stop(".pem file does not exist")
  }

  chmodScript <- paste("chmod 400", pem_path)
  system(chmodScript)
}

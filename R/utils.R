wait_file_exists <- function(filename, retries = 50) {
  while(!file.exists(filename) && retries >= 0) {
    retries <- retries  - 1;
    Sys.sleep(0.1)
  }

  file.exists(filename)
}

is.installed <- function(package) {
  is.element(package, installed.packages()[,1])
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

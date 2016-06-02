#' @importFrom utils browseURL download.file head installed.packages tail untar write.csv
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

validate_pem <- function(pemFile) {
  if (!file.exists(pemFile)) {
    stop(".pem file does not exist")
  }

  if (.Platform$OS.type == "unix") {
    chmodScript <- paste("chmod 400", pemFile)
    system(chmodScript)
  }
}

is_java_available <- function() {
  java_home <- Sys.getenv("JAVA_HOME", unset = NA)
  if (!is.na(java_home))
    java <- file.path(java_home, "bin", "java")
  else
    java <- Sys.which("java")
  nzchar(java)
}

java_install_url <- function() {
  "https://www.java.com/en/"
}

starts_with <- function(lhs, rhs) {
  if (nchar(lhs) < nchar(rhs))
    return(FALSE)
  identical(substring(lhs, 1, nchar(rhs)), rhs)
}

aliased_path <- function(path) {
  home <- path.expand("~/")
  if (starts_with(path, home))
    path <- file.path("~", substring(path, nchar(home) + 1))
  path
}

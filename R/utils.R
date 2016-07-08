#' @import methods
#' @import DBI
#' @importFrom utils browseURL download.file head installed.packages tail untar write.csv
NULL

.globals <- new.env(parent = emptyenv())

is.installed <- function(package) {
  is.element(package, installed.packages()[,1])
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

transpose_list <- function(list) {
  do.call(Map, c(c, list, USE.NAMES = FALSE))
}

random_string <- function(prefix = "table") {
  basename(tempfile(prefix))
}

"%||%" <- function(x, y) {
  if (is.null(x)) y else x
}

is_spark_v2 <- function(scon) {
  spark_version(scon) >= "2.0.0"
}

printf <- function(fmt, ...) {
  cat(sprintf(fmt, ...))
}

spark_require_version <- function(sc, required, module = NULL) {
  
  # guess module based on calling function
  if (is.null(module)) {
    call <- sys.call(sys.parent())
    module <- as.character(call[[1]])
  }
  
  # check and report version requirements
  version <- spark_version(sc)
  if (version < required) {
    fmt <- "'%s' requires Spark %s but you are using Spark %s"
    msg <- sprintf(fmt, module, required, version)
    stop(msg, call. = FALSE)
  }
  
  TRUE
}

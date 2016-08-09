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

regex_replace <- function(string, ...) {
  dots <- list(...)
  nm <- names(dots)
  for (i in seq_along(dots))
    string <- gsub(nm[[i]], dots[[i]], string, perl = TRUE)
  string
}

spark_sanitize_names <- function(names) {

  # sanitize names by default, but opt out with global option
  if (!isTRUE(getOption("sparklyr.sanitize.column.names", TRUE)))
    return(names)

  # begin transforming names
  oldNames <- newNames <- names

  # use 'iconv' to translate names to ASCII if possible
  newNames <- unlist(lapply(newNames, function(name) {

    # attempt to translate to ASCII
    transformed <- tryCatch(
      iconv(name, to = "ASCII//TRANSLIT"),
      error = function(e) NA
    )

    # on success, return the transformed name
    if (!is.na(transformed))
      transformed
    else
      name
  }))

  # replace spaces with '_', and discard other characters
  newNames <- regex_replace(
    newNames,
    "^\\s*|\\s*$" = "",
    "[\\s.]+"        = "_",
    "[^\\w_]"     = "",
    "^(\\W)"      = "V\\1"
  )

  # ensure new names are unique
  newNames <- make.unique(newNames, sep = "_")

  # report translations
  if (isTRUE(getOption("sparklyr.verbose", TRUE))) {

    changedIdx <- which(oldNames != newNames)
    if (length(changedIdx)) {

      changedOldNames <- oldNames[changedIdx]
      changedNewNames <- newNames[changedIdx]

      nLhs <- max(nchar(changedOldNames))
      nRhs <- max(nchar(changedNewNames))

      lhs <- sprintf(paste("%-", nLhs + 2, "s", sep = ""), shQuote(changedOldNames))
      rhs <- sprintf(paste("%-", nRhs + 2, "s", sep = ""), shQuote(changedNewNames))

      n <- floor(log10(max(changedIdx)))
      index <- sprintf(paste("(#%-", n, "s)", sep = ""), changedIdx)

      msg <- paste(
        "The following columns have been renamed:",
        paste("-", lhs, "=>", rhs, index, collapse = "\n"),
        sep = "\n"
      )

      message(msg)

    }
  }

  newNames
}

# normalize a path we are going to send to spark (pass mustWork = FALSE
# so that e.g. hdfs:// and s3n:// paths don't produce a warning). note
# that this will take care of path.expand ("~") as well as converting
# relative paths to absolute (necessary since the path will be read by
# another process that has a different current working directory)
spark_normalize_path <- function(path) {
  normalizePath(path, mustWork = FALSE)
}

stopf <- function(fmt, ..., call. = TRUE, domain = NULL) {
  stop(simpleError(
    sprintf(fmt, ...),
    if (call.) sys.call(sys.parent())
  ))
}

enumerate <- function(object, f, ...) {
  nm <- names(object)
  result <- lapply(seq_along(object), function(i) {
    f(nm[[i]], object[[i]], ...)
  })
  names(result) <- names(object)
  result
}

path_program <- function(program, fmt = NULL) {
  fmt <- fmt %||% "program '%s' is required but not available on the path"
  path <- Sys.which(program)
  if (!nzchar(path))
    stopf(fmt, program, call. = FALSE)
  path
}

infer_active_package_name <- function() {
  root <- rprojroot::find_package_root_file()
  dcf <- read.dcf(file.path(root, "DESCRIPTION"), all = TRUE)
  dcf$Package
}

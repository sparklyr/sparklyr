is.installed <- function(package) {
  is.element(package, installed.packages()[,1])
}

get_java <- function(throws = FALSE) {
  java_home <- Sys.getenv("JAVA_HOME", unset = NA)
  if (!is.na(java_home)) {
    java <- file.path(java_home, "bin", "java")
    if (identical(.Platform$OS.type, "windows")) {
      java <- paste0(java, ".exe")
    }
    if (!file.exists(java)) {
      if (throws) {
        stop("Java is required to connect to Spark. ",
             "JAVA_HOME is set but does not point to a valid version. ",
             "Please fix JAVA_HOME or reinstall from: ",
             java_install_url())
      }
      java <- ""
    }
  } else
    java <- Sys.which("java")
  java
}

is_java_available <- function() {
  nzchar(get_java())
}

validate_java_version_line <- function(master, version) {
  if (length(version) < 1)
    stop("Java version not detected. Please download and install Java from ",
         java_install_url())

  # find line with version info
  versionLine <- version[grepl("version", version)]
  if (length(versionLine) != 1)
    stop("Java version detected but couldn't parse version from ", paste(version, collapse = " - "))

  # transform to usable R version string
  splat <- strsplit(versionLine, "\\s+", perl = TRUE)[[1]]

  splatVersion <- splat[grepl("9|[0-9]+\\.[0-9]+\\.[0-9]+", splat)]
  if (length(splatVersion) != 1)
    stop("Java version detected but couldn't parse version from: ", versionLine)

  parsedVersion <- regex_replace(
    splatVersion,
    "^\"|\"$" = "",
    "_" = ".",
    "[^0-9.]+" = ""
  )

  if (!is.character(parsedVersion) || nchar(parsedVersion) < 1)
    stop("Java version detected but couldn't parse version from: ", versionLine)

  # ensure Java 1.7 or higher
  if (compareVersion(parsedVersion, "1.7") < 0)
    stop("Java version", parsedVersion, " detected but 1.7+ is required. Please download and install Java from ",
         java_install_url())

  if (compareVersion(parsedVersion, "1.9") >= 0 && spark_master_is_local(master)  && !getOption("sparklyr.java9", FALSE)) {
    stop(
      "Java 9 is currently unsupported in Spark distributions unless you manually install Hadoop 2.8 ",
      "and manually configure Spark. Please consider uninstalling Java 9 and reinstalling Java 8. ",
      "To override this failure set 'options(sparklyr.java9 = TRUE)'.")
  }
}

validate_java_version <- function(master, spark_home) {
  # if someone sets SPARK_HOME and we are not in local more, assume Java
  # is available since some systems.
  # (e.g. CDH) use versions of java not discoverable through JAVA_HOME.
  if (!spark_master_is_local(master) && !is.null(spark_home) && nchar(spark_home) > 0)
    return(TRUE)

  # find the active java executable
  java <- get_java(throws = TRUE)
  if (!nzchar(java))
    stop("Java is required to connect to Spark. Please download and install Java from ",
         java_install_url())

  # query its version
  version <- system2(java, "-version", stderr = TRUE, stdout = TRUE)
  validate_java_version_line(master, version)

  TRUE
}

java_install_url <- function() {
  "https://www.java.com/en/"
}

utils_starts_with <- function(lhs, rhs) {
  if (nchar(lhs) < nchar(rhs))
    return(FALSE)
  identical(substring(lhs, 1, nchar(rhs)), rhs)
}

aliased_path <- function(path) {
  home <- path.expand("~/")
  if (utils_starts_with(path, home))
    path <- file.path("~", substring(path, nchar(home) + 1))
  path
}

transpose_list <- function(list) {
  do.call(Map, c(c, list, USE.NAMES = FALSE))
}

#' Random string generation
#'
#' Generate a random string with a given prefix.
#'
#' @param prefix A length-one character vector.
#' @export
random_string <- function(prefix = "table") {
  basename(tempfile(prefix))
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
  verbose <- sparklyr_boolean_option(
    "sparklyr.sanitize.column.names.verbose",
    "sparklyr.verbose"
  )

  if (verbose) {

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

# normalizes a path that we are going to send to spark but avoids
# normalizing remote identifiers like hdfs:// or s3n://. note
# that this will take care of path.expand ("~") as well as converting
# relative paths to absolute (necessary since the path will be read by
# another process that has a different current working directory)
spark_normalize_path <- function(path) {
  # don't normalize paths that are urls
  if (grepl("[a-zA-Z]+://", path)) {
    path
  }
  else {
    normalizePath(path, mustWork = FALSE)
  }
}

stopf <- function(fmt, ..., call. = TRUE, domain = NULL) {
  stop(simpleError(
    sprintf(fmt, ...),
    if (call.) sys.call(sys.parent())
  ))
}

warnf <- function(fmt, ..., call. = TRUE, immediate. = FALSE) {
  warning(sprintf(fmt, ...), call. = call., immediate. = immediate.)
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

split_chunks <- function(x, chunk_size) {

  # return early when chunk_size > length of vector
  n <- length(x)
  if (n <= chunk_size)
    return(list(x))

  # compute ranges for subsetting
  starts <- seq(1, n, by = chunk_size)
  ends   <- c(seq(chunk_size, n - 1, by = chunk_size), n)

  # apply our subsetter
  mapply(function(start, end) {
    x[start:end]
  }, starts, ends, SIMPLIFY = FALSE, USE.NAMES = FALSE)
}

remove_class <- function(object, class) {
  classes <- attr(object, "class")
  newClasses <- classes[!classes %in% c(class)]

  attr(object, "class") <- newClasses
  object
}

sparklyr_boolean_option <- function(...) {

  for (name in list(...)) {
    value <- getOption(name) %||% FALSE
    if (length(value) == 1 && isTRUE(as.logical(value)))
      return(TRUE)
  }

  FALSE
}

sparklyr_verbose <- function(...) {
  sparklyr_boolean_option(..., "sparklyr.verbose")
}

trim_whitespace <- function(strings) {
  gsub("^[[:space:]]*|[[:space:]]*$", "", strings)
}


split_separator <- function(sc) {
  if (inherits(sc, "livy_connection"))
    list(regexp = "\\|~\\|", plain = "|~|")
  else
    list(regexp = "\31", plain = "\31")
}

resolve_fn <- function(fn, ...) {
  if (is.function(fn)) fn(...) else fn
}

is.tbl_spark <- function(x) {
  inherits(x, "tbl_spark")
}

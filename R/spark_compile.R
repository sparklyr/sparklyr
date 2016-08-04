#' Compiles scala sources and packages into a jar file
#'
#' @export
#' @param name The name of the target jar
#' @param spark_home Spark version
#'
#' @import rprojroot
#' @import digest
#'
#' @keywords internal
spark_compile <- function(name, spark_home) {
  spark_version <- spark_version_from_home(spark_home)
  version_numeric <- gsub("[-_a-zA-Z]", "", spark_version)
  version_sufix <- gsub("\\.|[-_a-zA-Z]", "", spark_version)
  jar_name <- paste0(name, "-", version_numeric, ".jar")

  root <- rprojroot::find_package_root_file()

  jar_path <- file.path(root, "inst", "java", jar_name)
  scala_files <- lapply(
    Filter(
      function(e) {
        # if filename has version only include version being built
        if (grepl(".*_\\d+\\.scala", e)) {
          grepl(version_sufix, e)
        }
        else {
          grepl(".*\\.scala$", e)
        }
      },
      list.files(file.path(root, "inst", "scala"))
    ),
    function(e) file.path(root, "inst", "scala", e)
  )
  scala_files_digest <- file.path(root, paste0(
    "inst/scala/sparklyr-", version_numeric, ".md5"
  ))

  scala_files_contents <- paste(lapply(scala_files, function(e) readLines(e)))
  scala_files_contents_path <- tempfile()
  scala_files_contents_file <- file(scala_files_contents_path, "w")
  writeLines(scala_files_contents, scala_files_contents_file)
  close(scala_files_contents_file)

  # Bail if files havent changed
  md5 <- tools::md5sum(scala_files_contents_path)
  if (file.exists(scala_files_digest) && file.exists(jar_path)) {
    contents <- readChar(scala_files_digest, file.info(scala_files_digest)$size, TRUE)
    if (identical(contents, md5[[scala_files_contents_path]])) {
      return()
    }
  }

  message("** building '", jar_name, "' ...")

  cat(md5, file = scala_files_digest)

  execute <- function(...) {
    cmd <- paste(...)
    message("*** ", cmd)
    system(cmd)
  }

  if (!nzchar(Sys.which("scalac")))
    stop("failed to discover 'scalac' on the PATH")

  if (!nzchar(Sys.which("jar")))
    stop("failed to discover 'jar' on the PATH")

  # Work in temporary directory (as temporary class files
  # will be generated within there)
  dir <- file.path(tempdir(), paste0(name, "-", version_sufix, "-scala-compile"))
  if (!file.exists(dir))
    if (!dir.create(dir))
      stop("Failed to create '", dir, "'")
  owd <- setwd(dir)

  # list jars in the installation folder
  candidates <- c("jars", "lib")
  jars <- NULL
  for (candidate in candidates) {
    jars <- list.files(
      file.path(spark_home, candidate),
      full.names = TRUE,
      pattern = "jar$"
    )

    if (length(jars))
      break
  }

  if (!length(jars))
    stop("failed to discover Spark jars")

  # construct classpath
  CLASSPATH <- paste(jars, collapse = .Platform$path.sep)

  # ensure 'inst/java' exists
  inst_java_path <- file.path(root, "inst/java")
  if (!file.exists(inst_java_path))
    if (!dir.create(inst_java_path, recursive = TRUE))
      stop("failed to create directory '", inst_java_path, "'")

  # call 'scalac' compiler
  classpath <- Sys.getenv("CLASSPATH")

  # set CLASSPATH environment variable rather than passing
  # in on command line (mostly aesthetic)
  Sys.setenv(CLASSPATH = CLASSPATH)
  execute("scalac", paste(shQuote(scala_files), collapse = " "))
  Sys.setenv(CLASSPATH = classpath)

  # call 'jar' to create our jar
  class_files <- file.path(name, list.files(name, pattern = "class$"))
  execute("jar cf", jar_path, paste(shQuote(class_files), collapse = " "))

  # double-check existence of jar
  if (file.exists(jar_path)) {
    message("*** ", basename(jar_path), " successfully created.")
  } else {
    stop("*** failed to create ", jar_name)
  }

  setwd(owd)
}

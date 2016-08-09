#' Compile Scala sources into a Java Archive (jar)
#'
#' Given a set of \code{scala} source files, compile them
#' into a Java Archive (\code{jar}).
#'
#' @param name The name to assign to the target \code{jar}.
#' @param spark_home The path to the Spark sources to be used
#'   alongside compilation.
#' @param filter An optional function, used to filter out discovered \code{scala}
#'   files during compilation. This can be used to ensure that e.g. certain files
#'   are only compiled with certain versions of Spark, and so on.
#' @param scalac The path to the \code{scalac} program to be used, for
#'   compilation of \code{scala} files.
#' @param jar The path to the \code{jar} program to be used, for
#'   generating of the resulting \code{jar}.
#'
#' @import rprojroot
#' @import digest
#'
#' @keywords internal
#' @export
spark_compile <- function(jar_name,
                          spark_home,
                          filter = function(files) files,
                          scalac = NULL,
                          jar = NULL)
{
  scalac <- scalac %||% path_program("scalac")
  jar    <- jar %||% path_program("jar")

  scalac_version <- get_scalac_version(scalac)
  spark_version <- numeric_version(spark_version_from_home(spark_home))

  root <- rprojroot::find_package_root_file()

  java_path <- file.path(root, "inst/java")
  jar_path <- file.path(java_path, jar_name)

  scala_path <- file.path(root, "inst/scala")
  scala_files <- list.files(scala_path, pattern = "scala$", full.names = TRUE)

  # apply user filter to scala files
  scala_files <- filter(scala_files)

  message("==> using scalac ", scalac_version)
  message("==> building against Spark ", spark_version)
  message("==> building '", jar_name, "' ...")

  execute <- function(...) {
    cmd <- paste(...)
    message("==> ", cmd)
    system(cmd)
  }

  # work in temporary directory
  dir <- tempfile(sprintf("scalac-%s", jar_name))
  ensure_directory(dir)
  owd <- setwd(dir)
  on.exit(setwd(owd), add = TRUE)

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
  ensure_directory(inst_java_path)

  # call 'scalac' with CLASSPATH set
  classpath <- Sys.getenv("CLASSPATH")
  Sys.setenv(CLASSPATH = CLASSPATH)
  on.exit(Sys.setenv(CLASSPATH = classpath), add = TRUE)
  status <- execute("scalac", paste(shQuote(scala_files), collapse = " "))
  if (status)
    stop("==> failed to compile Scala source files")

  # call 'jar' to create our jar
  class_files <- list.files(pattern = "class$", recursive = TRUE, full.names = TRUE)
  class_files <- sub("./", "", class_files)
  status <- execute("jar cf", jar_path, paste(shQuote(class_files), collapse = " "))
  if (status)
    stop("==> failed to build Java Archive")

  # double-check existence of jar
  if (!file.exists(jar_path))
    stop("==> failed to create ", jar_name)

  message("==> ", basename(jar_path), " successfully created\n")
  TRUE
}

#' Compile Scala sources into a Java Archive (jar)
#'
#' Compile the \code{scala} source files contained within an \R package
#' into a Java Archive (\code{jar}) file that can be loaded and used within
#' a Spark environment.
#'
#' @param package The path to an \R package.
#' @param spark_versions The Spark versions to build against. When \code{NULL},
#'   builds against all Spark versions discovered with
#'   \code{\link{spark_available_versions}}.
#' @param scalac The path to the \code{scalac} compiler to be used. When unset,
#'   \code{scalac} will be discovered on the PATH.
#' @param jar The path to the \code{jar} Java archive tool to be used.
#'   When unset, \code{jar} will be discovered on the PATH.
#'
#' @import rprojroot
#' @import digest
#'
#' @keywords internal
#' @export
compile_package_jars <- function(package = rprojroot::find_package_root_file(),
                                 compilation_spec = default_compilation_spec())
{
  if (!is.list(compilation_spec))
    compilation_spec <- list(compilation_spec)

  for (spec in compilation_spec) {
    spec <- as.list(spec)
    spark_compile(
      jar_name = spec$jar_name,
      spark_home = spec$spark_home,
      scalac = spec$scalac_path
    )
  }
}

spark_compilation_spec <- function(spark_version = NULL,
                                   spark_home = NULL,
                                   scalac_path = NULL,
                                   jar_name = NULL)
{
  spark_home    <- spark_home %||% spark_home_dir(spark_version)
  spark_version <- spark_version %||% spark_version_from_home(spark_home)

  c(spark_version = spark_version,
    spark_home = spark_home,
    scalac_path = scalac_path,
    jar_name = jar_name)
}

default_compilation_spec <- function(pkg = infer_active_package_name()) {
  list(
    spark_compilation_spec(
      spark_version = "1.6.1",
      scalac_path = "/usr/local/scala/scala-2.10.6/bin/scalac",
      jar_name = sprintf("%s-1.6.jar", pkg)
    ),
    spark_compilation_spec(
      spark_version = "2.0.0",
      scalac_path = "/usr/local/scala/scala-2.11.8/bin/scalac",
      jar_name = sprintf("%s-2.0.jar", pkg)
    )
  )
}

get_scalac_version <- function(scalac = Sys.which("scalac")) {
  # TODO: shell redirection won't work on Windows unless we go through shell
  cmd <- paste(shQuote(scalac), "-version 2>&1")
  version_string <- system(cmd, intern = TRUE)
  splat <- strsplit(version_string, "\\s+", perl = TRUE)[[1]]
  splat[[4]]
}

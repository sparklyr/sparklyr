sparklyr_jar_spec_list <- function() {
  list(
    list(spark = "1.5.2", scala = "2.10", remove_srcs = TRUE),
    list(spark = "1.6.0", scala = "2.10", scala_filter = "1.6.1"),
    list(spark = "2.0.0", scala = "2.11"),
    list(spark = "2.3.0", scala = "2.11"),
    list(spark = "2.4.0", scala = "2.11"),
    list(spark = "2.4.0", scala = "2.12"),
    list(spark = "3.4.0", scala = "2.12", jar_name = "sparklyr-master-2.12.jar")
  )
}

sparklyr_jar_verify_spark <- function(install = TRUE) {
  spec_list <- sparklyr_jar_spec_list()
  installed_vers <- spark_installed_versions()
  invisible(
    lapply(
      spec_list,
      function(x){
        if(!(x$spark %in% installed_vers$spark)) {
          message("- Spark version ", x$spark, " - Not found")
          if(install) spark_install(x$spark)
        } else {
          message("- Spark version ", x$spark, " - Ok")
        }
      }
    )
  )
}

#' Compile Scala sources into a Java Archive
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
#' @param jar_dep An optional list of additional \code{jar} dependencies.
#' @param embedded_srcs Embedded source file(s) under \code{<R package root>/java} to
#'   be included in the root of the resulting jar file as resources
#'
#' @import digest
#'
#' @keywords internal
#' @export
spark_compile <- function(jar_name,
                          spark_home = NULL,
                          filter = NULL,
                          scalac = NULL,
                          jar = NULL,
                          jar_dep = NULL,
                          embedded_srcs = "embedded_sources.R") {
  default_install <- spark_install_find()
  spark_home <- if (is.null(spark_home) && !is.null(default_install)) {
    spark_install_find()$sparkVersionDir
  } else {
    spark_home
  }

  scalac <- scalac %||% path_program("scalac")
  jar <- jar %||% path_program("jar")

  scalac_version <- get_scalac_version(scalac)
  spark_version <- numeric_version(spark_version_from_home(spark_home))

  root <- package_root()

  env_jar_path <- Sys.getenv("R_SPARKINSTALL_COMPILE_JAR_PATH", unset = NA)
  if(is.na(env_jar_path)) {
    java_path <- file.path(root, "inst/java")
  } else {
    if(!dir.exists(env_jar_path)) dir.create(env_jar_path)
    java_path <- env_jar_path
  }

  jar_path <- file.path(java_path, jar_name)

  scala_path <- file.path(root, "java")
  scala_files <- list.files(scala_path,
    pattern = "scala$",
    full.names = TRUE,
    recursive = TRUE
  )

  # apply user filter to scala files
  if (is.function(filter)) {
    scala_files <- filter(scala_files)
  }

  message("==> Using scalac: ", scalac_version)
  message("==> Building against Spark: ", spark_version)
  message("==> Building: '", jar_name, "\n")

  execute <- function(...) {
    cmd <- paste(...)
    message("==> System command: ", cmd, "\n")
    system(cmd)
  }

  # work in temporary directory
  dir <- tempfile(sprintf("scalac-%s-", sub("-.*", "", jar_name)))
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

    if (length(jars)) {
      break
    }
  }

  jars <- c(jars, jar_dep)

  if (!length(jars)) {
    stop("failed to discover Spark jars")
  }

  # construct classpath
  CLASSPATH <- paste(jars, collapse = .Platform$path.sep)

  # ensure 'inst/java' exists
  inst_java_path <- file.path(root, "inst/java")
  ensure_directory(inst_java_path)

  # copy embedded sources to current working directory
  message("==> Embedded source(s): ", paste(embedded_srcs, collapse = ", "), "\n")
  ensure_directory("sparklyr")
  for (src in embedded_srcs) {
    file.copy(file.path(scala_path, src), "sparklyr")
  }

  # call 'scalac' with CLASSPATH set
  classpath <- Sys.getenv("CLASSPATH")
  Sys.setenv(CLASSPATH = CLASSPATH)
  on.exit(Sys.setenv(CLASSPATH = classpath), add = TRUE)
  scala_files_quoted <- paste(shQuote(scala_files), collapse = " ")
  optflag <- ifelse(grepl("2.12", scalac_version), "-opt:l:default", "-optimise")
  status <- execute(shQuote(scalac), optflag, "-deprecation", "-feature", scala_files_quoted)

  if (status) {
    stop("==> Failed to compile Scala source files")
  }

  # call 'jar' to create our jar
  status <- execute(shQuote(jar), "cf", shQuote(jar_path), ".")
  if (status) {
    stop("==> Failed to build Java Archive")
  }

  # double-check existence of jar
  if (!file.exists(jar_path)) {
    stop("==> failed to create ", jar_name)
  }

  message("==> ", basename(jar_path), " successfully created\n")
  TRUE
}

#' Compile Scala sources into a Java Archive (jar)
#'
#' Compile the \code{scala} source files contained within an \R package
#' into a Java Archive (\code{jar}) file that can be loaded and used within
#' a Spark environment.
#'
#' @param ... Optional compilation specifications, as generated by
#'   \code{spark_compilation_spec}. When no arguments are passed,
#'   \code{spark_default_compilation_spec} is used instead.
#' @param spec An optional list of compilation specifications. When
#'   set, this option takes precedence over arguments passed to
#'   \code{...}.
#'
#' @export
compile_package_jars <- function(..., spec = NULL) {

  # unpack compilation specification
  spec <- spec %||% list(...)
  if (!length(spec)) {
    spec <- spark_default_compilation_spec()
  }

  if (!is.list(spec[[1]])) {
    spec <- list(spec)
  }

  for (el in spec) {
    el <- as.list(el)
    spark_version <- el$spark_version
    spark_home <- el$spark_home
    jar_name <- el$jar_name
    scalac_path <- el$scalac_path
    filter <- el$scala_filter
    jar_path <- el$jar_path
    jar_dep <- el$jar_dep
    embedded_srcs <- el$embedded_srcs

    # try to automatically download + install Spark
    if (is.null(spark_home) && !is.null(spark_version)) {
      if (spark_version == "master") {
        installInfo <- spark_install_find(
          version = NULL,
          hadoop_version = NULL,
          installed_only = TRUE,
          latest = FALSE
          )
        spark_version <- installInfo$sparkVersion
        spark_home <- installInfo$sparkVersionDir
      } else {
        message("==> downloading Spark ", spark_version)
        spark_install(spark_version, verbose = TRUE)
        spark_home <- spark_home_dir(spark_version)
      }
    }

    spark_compile(
      jar_name = jar_name,
      spark_home = spark_home,
      filter = filter,
      scalac = scalac_path,
      jar = jar_path,
      jar_dep = jar_dep,
      embedded_srcs = embedded_srcs
    )
  }
}

#' Define a Spark Compilation Specification
#'
#' For use with \code{\link{compile_package_jars}}. The Spark compilation
#' specification is used when compiling Spark extension Java Archives, and
#' defines which versions of Spark, as well as which versions of Scala, should
#' be used for compilation.
#'
#' Most Spark extensions won't need to define their own compilation specification,
#' and can instead rely on the default behavior of \code{compile_package_jars}.
#'
#' @param spark_version The Spark version to build against. This can
#'   be left unset if the path to a suitable Spark home is supplied.
#' @param spark_home The path to a Spark home installation. This can
#'   be left unset if \code{spark_version} is supplied; in such a case,
#'   \code{sparklyr} will attempt to discover the associated Spark
#'   installation using \code{\link{spark_home_dir}}.
#' @param scalac_path The path to the \code{scalac} compiler to be used
#'   during compilation of your Spark extension. Note that you should
#'   ensure the version of \code{scalac} selected matches the version of
#'   \code{scalac} used with the version of Spark you are compiling against.
#' @param scala_filter An optional \R function that can be used to filter
#'   which \code{scala} files are used during compilation. This can be
#'   useful if you have auxiliary files that should only be included with
#'   certain versions of Spark.
#' @param jar_name The name to be assigned to the generated \code{jar}.
#' @param jar_path The path to the \code{jar} tool to be used
#'   during compilation of your Spark extension.
#' @param jar_dep An optional list of additional \code{jar} dependencies.
#' @param embedded_srcs Embedded source file(s) under \code{<R package root>/java} to
#'   be included in the root of the resulting jar file as resources
#'
#' @export
spark_compilation_spec <- function(spark_version = NULL,
                                   spark_home = NULL,
                                   scalac_path = NULL,
                                   scala_filter = NULL,
                                   jar_name = NULL,
                                   jar_path = NULL,
                                   jar_dep = NULL,
                                   embedded_srcs = "embedded_sources.R") {
  spark_home <- spark_home %||% spark_home_dir(spark_version)
  spark_version <- spark_version %||% spark_version_from_home(spark_home)

  list(
    spark_version = spark_version,
    spark_home = spark_home,
    scalac_path = scalac_path,
    scala_filter = scala_filter,
    jar_name = jar_name,
    jar_path = jar_path,
    jar_dep = jar_dep,
    embedded_srcs = embedded_srcs
  )
}

find_jar <- function() {
  env_java_jome <- Sys.getenv("JAVA_HOME")
  if (nchar(env_java_jome) > 0) {
    p_jar <- normalizePath(
      file.path(env_java_jome, "bin", "jar"),
      mustWork = FALSE
      )
    p_res <- NULL
    if(file.exists(p_jar)) p_res <- p_jar
    if(is.null(p_res)) p_res <- system2("which", "jar", stdout = TRUE)
    p_res
  } else {
    NULL
  }
}

#' Default Compilation Specification for Spark Extensions
#'
#' This is the default compilation specification used for
#' Spark extensions, when used with \code{\link{compile_package_jars}}.
#'
#' @param pkg The package containing Spark extensions to be compiled.
#' @param locations Additional locations to scan. By default, the
#'   directories \code{/opt/scala} and \code{/usr/local/scala} will
#'   be scanned.
#'
#' @export
spark_default_compilation_spec <- function(pkg = infer_active_package_name(),
                                           locations = NULL) {

  spec_list <- sparklyr_jar_spec_list()

  jar_location <- find_jar()

  lapply(
    spec_list,
    function(x) {
      args <- list(
        spark_version = x$spark,
        scalac_path = find_scalac(x$scala, locations),
        jar_name = sprintf("%s-%s-%s.jar", pkg, substr(x$spark, 1, 3), x$scala),
        jar_path = jar_location,
        scala_filter = make_version_filter(x$spark)
      )

      if(!is.null(x$jar_name)) args$jar_name <- x$jar_name
      if(!is.null(x$scala_filter)) args$scala_filter <- make_version_filter(x$scala_filter)
      if(!is.null(x$remove_srcs)) args <- c(args, list(embedded_srcs = c()))
      do.call(spark_compilation_spec, args)
    }
  )
}

#' Downloads default Scala Compilers
#'
#' \code{compile_package_jars} requires several versions of the
#' scala compiler to work, this is to match Spark scala versions.
#' To help setup your environment, this function will download the
#' required compilers under the default search path.
#'
#' See \code{find_scalac} for a list of paths searched and used by
#' this function to install the required compilers.
#'
#' @param dest_path The destination path where scalac will be
#'   downloaded to.
#'
#' @export
download_scalac <- function(dest_path = NULL) {
  if (is.null(dest_path)) {
    dest_path <- scalac_default_locations()[[1]]
  }

  if (!dir.exists(dest_path)) {
    dir.create(dest_path, recursive = TRUE)
  }

  ext <- ifelse(os_is_windows(), "zip", "tgz")

  download_urls <-  paste0(
    c(
      "https://downloads.lightbend.com/scala/2.12.10/scala-2.12.10",
      "https://downloads.lightbend.com/scala/2.11.8/scala-2.11.8",
      "https://downloads.lightbend.com/scala/2.10.6/scala-2.10.6"
    ),
    ".",
    ext
  )

  lapply(download_urls, function(download_url) {
    dest_file <- file.path(dest_path, basename(download_url))

    if (!dir.exists(dirname(dest_file))) {
      dir.create(dirname(dest_file), recursive = TRUE)
    }

    if(!file.exists(dest_file)) {
      download_file(download_url, destfile = dest_file)
    }

    if (ext == "zip") {
      unzip(dest_file, exdir = dest_path)
    } else {
      untar(dest_file, exdir = dest_path)
    }
  })
}

#' Discover the Scala Compiler
#'
#' Find the \code{scalac} compiler for a particular version of
#' \code{scala}, by scanning some common directories containing
#' \code{scala} installations.
#'
#' @param version The \code{scala} version to search for. Versions
#'   of the form \code{major.minor} will be matched against the
#'   \code{scalac} installation with version \code{major.minor.patch};
#'   if multiple compilers are discovered the most recent one will be
#'   used.
#' @param locations Additional locations to scan. By default, the
#'   directories \code{/opt/scala} and \code{/usr/local/scala} will
#'   be scanned.
#'
#' @export
find_scalac <- function(version, locations = NULL) {
  locations <- locations %||% scalac_default_locations()
  re_version <- paste("^", version, "(\\.[0-9]+)?$", sep = "")

  for (location in locations) {
    installs <- sort(list.files(location))

    versions <- sub("^scala-?", "", installs)
    matches <- grep(re_version, versions)
    if (!any(matches)) {
      next
    }

    index <- tail(matches, n = 1)
    install <- installs[index]
    scalac_path <- file.path(location, install, "bin/scalac")
    if (!file.exists(scalac_path)) {
      next
    }

    return(scalac_path)
  }

  stopf("failed to discover scala-%s compiler -- search paths were:\n%s",
    version,
    paste("-", shQuote(locations), collapse = "\n"),
    call. = FALSE
  )
}

scalac_default_locations <- function() {
  if (os_is_windows()) {
      path.expand("~/scala")
  } else {
    c(
      path.expand("~/scala"),
      "/usr/local/scala",
      "/opt/local/scala",
      "/opt/scala"
    )
  }
}

get_scalac_version <- function(scalac = Sys.which("scalac")) {
  cmd <- paste(shQuote(scalac), "-version 2>&1")
  version_string <- if (os_is_windows()) {
    shell(cmd, intern = TRUE)
  } else {
    system(cmd, intern = TRUE)
  }
  splat <- strsplit(version_string, "\\s+", perl = TRUE)[[1]]
  splat[[4]]
}

make_version_filter <- function(version_upper) {
  force(version_upper)

  function(files) {
    Filter(function(file) {
      maybe_version <- file %>%
        dirname() %>%
        basename() %>%
        strsplit("-") %>%
        unlist() %>%
        dplyr::last()

      if (grepl("([0-9]+\\.){2}[0-9]+", maybe_version)) {
        if (version_upper == "master") {
          use_file <- TRUE
        } else {
          use_file <- numeric_version(maybe_version) <= numeric_version(version_upper)
        }
        file_name <- basename(file)

        # is there is more than one file with the same name
        if (sum(file_name == basename(files)) > 1) {
          repeated_names <- Filter(function(e) grepl(file_name, e), files)
          other_versions <- repeated_names %>%
            dirname() %>%
            basename() %>%
            strsplit("-") %>%
            sapply(function(e) e[-1]) %>%
            numeric_version() %>%
            Filter(function(e) if (version_upper == "master") TRUE else e <= numeric_version(version_upper), .)

          # only use the file with the biggest version
          numeric_version(maybe_version) == max(other_versions)
        }
        else {
          use_file
        }
      } else {
        TRUE
      }
    }, files)
  }
}

#' list all sparklyr-*.jar files that have been built
list_sparklyr_jars <- function() {
  normalizePath(dir(
      file.path(package_root(), "inst", "java"),
      full.names = TRUE, pattern = "sparklyr-.+\\.jar"
    ))
}

package_root <- function() {
  rlang::check_installed("rprojroot")
  root <- get("find_package_root_file", envir = asNamespace("rprojroot"))
  root()
}

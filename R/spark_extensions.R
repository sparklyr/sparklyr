#' Register a Package that Implements a Spark Extension
#'
#' Registering an extension package will result in the package being
#' automatically scanned for spark dependencies when a connection to Spark is
#' created.
#'
#' @param package The package(s) to register.
#'
#' @note Packages should typically register their extensions in their
#'   \code{.onLoad} hook -- this ensures that their extensions are registered
#'   when their namespaces are loaded.
#'
#' @export
register_extension <- function(package) {
  genv_set_extension_packages(c(
    genv_get_extension_packages(),
    package
  ))
}

#' @rdname register_extension
#' @export
registered_extensions <- function() {
  genv_get_extension_packages()
}


#' Define a Spark dependency
#'
#' Define a Spark dependency consisting of a set of custom JARs, Spark packages,
#' and customized dbplyr SQL translation env.
#'
#' @param jars Character vector of full paths to JAR files.
#' @param packages Character vector of Spark packages names.
#' @param initializer Optional callback function called when initializing a connection.
#' @param catalog Optional location where extension JAR files can be downloaded for Livy.
#' @param repositories Character vector of Spark package repositories.
#' @param dbplyr_sql_variant Customization of dbplyr SQL translation env. Must be a
#'   named list of the following form:
#'   \code{
#'     list(
#'       scalar = list(scalar_fn1 = ..., scalar_fn2 = ..., <etc>),
#'       aggregate = list(agg_fn1 = ..., agg_fn2 = ..., <etc>),
#'       window = list(wnd_fn1 = ..., wnd_fn2 = ..., <etc>)
#'     )
#'     }
#'   See \link[dbplyr:sql_substr]{sql_variant} for details.
#' @param ... Additional optional arguments.
#'
#' @return An object of type `spark_dependency`
#'
#' @export
spark_dependency <- function(jars = NULL,
                             packages = NULL,
                             initializer = NULL,
                             catalog = NULL,
                             repositories = NULL,
                             dbplyr_sql_variant = NULL,
                             ...) {
  structure(class = "spark_dependency", list(
    jars = jars,
    packages = packages,
    initializer = initializer,
    catalog = catalog,
    repositories = repositories,
    dbplyr_sql_variant = dbplyr_sql_variant
  ))
}

spark_dependencies_from_extensions <- function(spark_version, scala_version, extensions, config) {
  scala_version <- numeric_version(
    scala_version %||% (
      if (spark_version < "2.0") {
        scala_version <- "2.10"
      } else if (spark_version < "3.0") {
        scala_version <- "2.11"
      } else {
        scala_version <- "2.12"
      })
  )
  jars <- character()
  packages <- character()
  initializers <- list()
  catalog_jars <- character()
  repositories <- character()
  dbplyr_sql_variant <- list(scalar = list(), aggregate = list(), window = list())

  for (extension in extensions) {
    dependencies <- spark_dependencies_from_extension(spark_version, scala_version, extension)
    for (dependency in dependencies) {
      jars <- c(jars, dependency$jars)
      packages <- c(packages, dependency$packages)
      initializers <- c(initializers, dependency$initializer)
      repositories <- c(repositories, dependency$repositories)

      for (x in c("scalar", "aggregate", "window")) {
        if (!is.null(dependency$dbplyr_sql_variant[[x]])) {
          for (fn in names(dependency$dbplyr_sql_variant[[x]])) {
            if (fn %in% names(dbplyr_sql_variant[[x]][[fn]])) {
              warning("overriding existing dbplyr sql translation for '", fn, "'")
            }
            dbplyr_sql_variant[[x]][[fn]] <- dependency$dbplyr_sql_variant[[x]][[fn]]
          }
        }
      }
      config_catalog <- spark_config_value(config, "sparklyr.extensions.catalog", TRUE)
      if (!identical(dependency$catalog, NULL) && !identical(config_catalog, FALSE)) {
        catalog_path <- dependency$catalog
        if (is.character(config_catalog)) catalog_path <- config_catalog

        if (!grepl("%s", config_catalog)) config_catalog <- paste0(config_catalog, "%s")

        catalog_jars <- c(
          catalog_jars,
          sprintf(config_catalog, basename(jars))
        )
      }
    }
  }

  list(
    jars = jars,
    packages = packages,
    initializers = initializers,
    catalog_jars = catalog_jars,
    repositories = repositories,
    dbplyr_sql_variant = dbplyr_sql_variant
  )
}

spark_dependencies_from_extension <- function(spark_version, scala_version, extension) {
  # attempt to find the function
  spark_dependencies <- tryCatch(
    {
      get("spark_dependencies", asNamespace(extension), inherits = FALSE)
    },
    error = function(e) {
      stop("spark_dependencies function not found within ",
        "extension package ", extension,
        call. = FALSE
      )
    }
  )

  # reduce the spark_version to just major and minor versions
  spark_version <- package_version2(spark_version)
  spark_version <- paste(spark_version$major, spark_version$minor, sep = ".")
  spark_version <- numeric_version(spark_version)

  # call the function
  dependency <- spark_dependencies(
    spark_version = spark_version,
    scala_version = scala_version
  )

  # if it's just a single dependency then wrap it in a list
  if (inherits(dependency, "spark_dependency")) {
    dependency <- list(dependency)
  }

  # return it
  dependency
}

#' Fallback to Spark Dependency
#'
#' Helper function to assist falling back to previous Spark versions.
#'
#' @param spark_version The Spark version being requested in \code{spark_dependencies}.
#' @param supported_versions The Spark versions that are supported by this extension.
#'
#' @return A Spark version to use.
#'
#' @export
spark_dependency_fallback <- function(spark_version, supported_versions) {
  supported <- supported_versions[package_version2(supported_versions) <= spark_version]
  sort(supported, decreasing = TRUE)[[1]]
}

sparklyr_jar_path <- function(spark_version, scala_version = NULL) {
  scala_version <- scala_version %||% (
    if (spark_version < "2.0") {
      "2.10"
    } else if (spark_version < "3.0") {
      "2.11"
    } else {
      "2.12"
    })

  spark_major_minor <- spark_version[1, 1:2]

  exact_jar <- sprintf("sparklyr-%s-%s.jar", spark_major_minor, scala_version)

  if (grepl("sparklyr-3\\.[5-9]-[0-9]+\\.[0-9]+\\.jar", exact_jar)) {
    # At the moment we ship sparklyr-3.0-2.12.jar as sparklyr-master-2.12.jar
    # for Databricks-related reasons, and do not duplicate the same jar file
    # twice under different names to avoid inflating the size of the R package,
    # and sparklyr-3.5-2.12.jar should be considered compatible with Spark 3.5+
    exact_jar <- "sparklyr-master-2.12.jar"
  }

  java_path <- system.file("java", package = "sparklyr")

  all_jars <- dir(java_path, pattern = "sparklyr")

  out <- ""

  # When it finds a JAR with the exact same name
  if (exact_jar %in% all_jars) {
    out <- file.path(java_path, exact_jar)
  }

  # Overrides when there is a environment variable set
  if (exists(".test_on_spark_master", envir = .GlobalEnv) && out == "") {
    out <- file.path(java_path, "sparklyr-master-2.12.jar")
  }

  # If no exact match, it looks for the Jar with the closes version
  if (spark_version > "1.6" && out == "") {
    spark_pattern <- "^sparklyr-|-[0-9]+\\.[0-9]+\\.jar$"
    all_versions <- sort(gsub(spark_pattern, "", all_jars), decreasing = TRUE)

    # Support preview versions and master builds
    all_versions <- all_versions[all_versions != "master"]
    all_versions <- gsub("-preview", "", all_versions)

    prev_versions <- all_versions[all_versions <= spark_version]

    if (length(prev_versions)) {
      out <- dir(
        java_path,
        pattern = paste0("sparklyr-", prev_versions[1]),
        full.names = TRUE
      )
    }
  }

  out
}

# sparklyr's own declared dependencies
spark_dependencies <- function(spark_version, scala_version) {
  spark_dependency(jars = sparklyr_jar_path(spark_version, scala_version))
}

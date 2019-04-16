#' Create Spark Extension
#'
#' Creates an R package ready to be used as an Spark extension.
#'
#' @param path Location where the extension will be created.
#'
#' @export
spark_extension <- function(path) {
  project_template(path)
}

project_template <- function(path, ...) {
  project_name <- tolower(basename(path))

  scala_path <- file.path(path, "java")
  r_path <- file.path(path, "R")

  main_scala <- c(
    paste("package", project_name),
    "",
    "object Main {",
    "  def hello() : String = {",
    "    \"Hello, world! - From Scala\"",
    "  }",
    "}"
  )

  main_r <- c(
    "#' @import sparklyr",
    "#' @export",
    paste(project_name, "_hello <- function(sc) {", sep = ""),
    paste("  sparklyr::invoke_static(sc, \"", project_name, ".Main\", \"hello\")", sep = ""),
    "}"
  )

  dependencies_r <- c(
    "spark_dependencies <- function(spark_version, scala_version, ...) {",
    "  sparklyr::spark_dependency(",
    "    jars = c(",
    "      system.file(",
    paste("        sprintf(\"java/", project_name, "-%s-%s.jar\", spark_version, scala_version),", sep = ""),
    paste("        package = \"", project_name, "\"", sep = ""),
    "      )",
    "    ),",
    "    packages = c(",
    "    )",
    "  )",
    "}",
    "",
    "#' @import sparklyr",
    ".onLoad <- function(libname, pkgname) {",
    "  sparklyr::register_extension(pkgname)",
    "}"
  )

  description_file <- c(
    paste("Package: ", project_name, sep = ""),
    "Type: Package",
    "Title: What the Package Does (Title Case)",
    "Version: 0.1.0",
    "Author: Who wrote it",
    "Maintainer: The package maintainer <yourself@somewhere.net>",
    "Description: More about what it does (maybe more than one line)",
    "  Use four spaces when indenting paragraphs within the Description.",
    "License: What license is it under?",
    "Encoding: UTF-8",
    "LazyData: true",
    "Depends:",
    "  R (>= 3.1.2)",
    "Imports:",
    "  sparklyr"
  )

  namespace_file <- c(
    paste("export(", project_name, "_hello)", sep = ""),
    "import(sparklyr)"
  )

  readme_file <- c(
    "---",
    "title: \"What the Package Does (Title Case)\"",
    "output:",
    "  github_document:",
    "    fig_width: 9",
    "    fig_height: 5",
    "---",
    "",
    "## Building",
    "",
    "First build this package, then build its jars by running:",
    "",
    "```{r eval=FALSE}",
    "sparklyr::compile_package_jars()",
    "```",
    "",
    "then build the R package as usual.",
    "",
    "## Getting Started",
    "",
    "Connect and test this package as follows:",
    "",
    "```{r}",
    paste("library(", project_name, ")", sep = ""),
    "library(sparklyr)",
    "sc <- spark_connect(master = \"local\")",
    "",
    paste(project_name, "_hello(sc)", sep = ""),
    "```",
    "",
    "```{r}",
    "spark_disconnect_all()",
    "```"
  )

  dir.create(scala_path, recursive = TRUE, showWarnings = FALSE)
  dir.create(r_path, recursive = TRUE, showWarnings = FALSE)

  writeLines(main_scala, con = file.path(scala_path, "main.scala"))
  writeLines(main_r, con = file.path(r_path, "main.R"))
  writeLines(dependencies_r, con = file.path(r_path, "dependencies.R"))
  writeLines(description_file, con = file.path(path, "DESCRIPTION"))
  writeLines(namespace_file, con = file.path(path, "NAMESPACE"))
  writeLines(readme_file, con = file.path(path, "README.Rmd"))

  TRUE
}

# nocov start

livy_sources_included <- function() {
  c(
    "/invoke\\.scala",
    "/tracker\\.scala",
    "/serializer\\.scala",
    "/stream\\.scala",
    "/sqlutils\\.scala",
    "/utils\\.scala",
    "/repartition\\.scala",
    "/tracker\\.scala",
    "/livyutils\\.scala",
    "/applyutils\\.scala",
    "/classutils\\.scala",
    "/fileutils\\.scala",
    "/rscript\\.scala",
    "/sources\\.scala",
    "/workercontext\\.scala",
    "/channel\\.scala",
    "/handler\\.scala",
    "/backend\\.scala",
    "/workerhelper\\.scala",
    "/workerapply\\.scala",
    "/workerrdd\\.scala",
    "/workerutils\\.scala",
    "/mlutils\\.scala",
    "/mlutils2\\.scala",
    "/bucketizerutils\\.scala"
    # New files might require entries to livy_load_scala_sources and globalClassMap
  )
}

livy_sources_refresh <- function() {
  root <- rprojroot::find_package_root_file()

  livyPath <- file.path(root, "inst/livy")
  scalaPath <- file.path(root, "java")

  scalaFiles <- list.files(scalaPath,
                           pattern = "scala$",
                           full.names = TRUE,
                           recursive = TRUE)

  includedPatterns <- livy_sources_included()
  includedFiles <- scalaFiles[grepl(paste(includedPatterns, collapse = "|"), scalaFiles)]

  lapply(includedFiles, function(sourcePath) {
    destinationPath <- file.path(livyPath, basename(dirname(sourcePath)),
                                 basename(sourcePath))

    lines <- readLines(sourcePath)

    if (!dir.exists(dirname(destinationPath)))
      dir.create(dirname(destinationPath), recursive = TRUE)

    write("//", file = destinationPath, append = FALSE)
    write("// This file was automatically generated using livy_sources_refresh()", file = destinationPath, append = TRUE)
    write("// Changes to this file will be reverted.", file = destinationPath, append = TRUE)
    write("//", file = destinationPath, append = TRUE)

    # remove unsupported commands from file
    lines <- lines[!grepl("^package sparklyr", lines)]

    lines <- gsub("^import sparklyr\\.", "import ", lines)

    lapply(lines, function(line) {
      write(line, file = destinationPath, append = TRUE)
    })
  })

  invisible(NULL)
}

# nocov end

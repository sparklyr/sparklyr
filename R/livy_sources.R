livy_sources_included <- function() {
  c(
    "/invoke\\.scala",
    "/tracker\\.scala",
    "/serializer\\.scala",
    "/stream\\.scala",
    "/sqlutils\\.scala",
    "/utils\\.scala"
  )
}

livy_sources_refresh <- function() {
  root <- rprojroot::find_package_root_file()

  livyPath <- file.path(root, "inst/livy")
  scalaPath <- file.path(root, "java")

  scalaFiles <- list.files(scalaPath, pattern = "scala$", full.names = TRUE)

  includedPatterns <- livy_sources_included()
  includedFiles <- scalaFiles[grepl(paste(includedPatterns, collapse = "|"), scalaFiles)]

  lapply(includedFiles, function(sourcePath) {
    destinationPath <- file.path(livyPath, basename(sourcePath))

    lines <- readLines(sourcePath)

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

base_dir <- tempdir()
iris_in_dir <- file.path(base_dir, "iris-in")
iris_in <- paste0("file://", iris_in_dir)
iris_out_dir <- file.path(base_dir, "iris-out")
iris_out <- paste0("file://", iris_out_dir)

test_stream <- function(description, test) {
  if (!dir.exists(iris_in_dir)) {
    dir.create(iris_in_dir)
  }

  if (dir.exists(iris_out_dir)) {
    unlink(iris_out_dir, recursive = TRUE)
  }

  write.table(
    iris,
    file.path(iris_in_dir, "iris.csv"),
    row.names = FALSE,
    sep = ";"
  )

  on.exit(unlink(iris_out_dir, recursive = TRUE))

  test_that(description, test)
}

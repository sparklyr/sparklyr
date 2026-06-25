expect_regexes <- function(printed, regexes) {
  for (i in seq_along(regexes)) {
    expect_gt(grep(regexes[[i]], printed[[i]]), 0)
  }
}

verify_table_src <- function(printed) {
  # dbplyr (>= 2.6.0) prints lazy tables with a `# A query:` header and a
  # separate `# Database:` line (previously a single `# Source: table<...>`
  # line). The `# Database:` line is filled by `db_connection_describe`.
  expect_output(
    printed,
    regexp = "# Database: spark_connection"
  )
}

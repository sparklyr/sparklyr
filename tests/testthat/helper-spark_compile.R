scalac_is_available <- function(version, download_path) {
  tryCatch(
    {
      find_scalac(version, download_path)
      TRUE
    },
    error = function(e) FALSE
  )
}

ensure_download_scalac <- function(download_path) {
  for (scala_version in c("2.10", "2.11", "2.12")) {
    if (!scalac_is_available(scala_version, download_path)) {
      download_scalac(download_path)
      break
    }
  }
}

scalac_download_path <- tempdir()

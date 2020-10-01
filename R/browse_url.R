browse_url <- function(url) {
  tryCatch(
    {
      url <- as.character(url)
      # Translate a local URL needs to an external one if necessary
      # (e.g., https://localhost:4040 -> https://<user>.rstudio.cloud/...)
      if (rstudioapi::isAvailable()) {
        if (!identical(substr(url, nchar(url), nchar(url)), "/")) {
          # append a trailing '/' if necessary
          url <- paste0(url, "/")
        }
        rstudioapi::translateLocalUrl(url, absolute = TRUE)
      } else {
        url
      }
    },
    error = function(e) as.character(url)
  ) %>%
    utils::browseURL()
}

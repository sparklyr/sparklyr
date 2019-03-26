create_hive_context <- function(sc) {
  UseMethod("create_hive_context")
}

apply_config <- function(object, params, method, prefix) {
  if (!length(params)) return(object)

  params %>%
    purrr::map_if(is.logical, ~ (if (.x) "true" else "false")) %>%
    purrr::map_chr(as.character) %>%
    purrr::imap(~ list(key = .y, value = .x)) %>%
    purrr::reduce(~ invoke(.x, method, paste0(prefix, .y$key), .y$value), .init = object)
}

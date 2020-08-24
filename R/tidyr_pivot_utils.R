canonicalize_spec <- function(spec) {
  if (!is.data.frame(spec)) {
    stop("`spec` must be a data frame", call. = FALSE)
  }

  if (!rlang::has_name(spec, ".name") || !rlang::has_name(spec, ".value")) {
    stop("`spec` must have `.name` and `.value` columns", call. = FALSE)
  }

  # Ensure .name and .value come first
  vars <- union(c(".name", ".value"), names(spec))
  spec[vars]
}

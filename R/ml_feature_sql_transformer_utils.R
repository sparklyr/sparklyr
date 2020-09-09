ft_extract_sql <- function(x) {
  get_base_name <- function(o) {
    if (!inherits(o$x, "ident")) {
      get_base_name(o$x)
    } else {
      o$x
    }
  }
  pattern <- paste0("\\b", get_base_name(x$ops), "\\b")

  gsub(pattern, "__THIS__", dbplyr::sql_render(x))
}

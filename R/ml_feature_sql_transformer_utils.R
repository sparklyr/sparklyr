ft_extract_sql <- function(x) {
  get_base_name <- function(o) {
    # TODO
    xx <- o$from %||% o$x
    if (!inherits(xx, "ident")) {
      get_base_name(xx)
    } else {
      xx
    }
  }
  pattern <- paste0("\\b", get_base_name(x$lazy_query), "\\b")

  gsub(pattern, "__THIS__", dbplyr::sql_render(x))
}

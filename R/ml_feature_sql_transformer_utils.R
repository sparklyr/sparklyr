ft_extract_sql <- function(x) {
  if (dbplyr_uses_ops()) {
    get_base_name <- function(o) {
      if (!inherits(o$x, "ident")) {
        get_base_name(o$x)
      } else {
        o$x
      }
    }

    pattern <- paste0("\\b", get_base_name(x$ops), "\\b")
  } else {
    get_base_name <- function(o) {
      if (!inherits(o$x, "lazy_base_query")) {
        get_base_name(o$x)
      } else {
        o$x$x
      }
    }

    pattern <- paste0("\\b", get_base_name(x$lazy_query), "\\b")
  }

  gsub(pattern, "__THIS__", dbplyr::sql_render(x))
}

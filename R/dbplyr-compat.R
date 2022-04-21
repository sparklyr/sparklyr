add_op_single <- function(...) {
  if (dbplyr_uses_ops()) {
    f <- utils::getFromNamespace("distinct.tbl_lazy", "dbplyr")
    f(...)
  }
}

add_filter <- function(...) {
  if (dbplyr_uses_ops()) {
    rlang::abort("Internal error")
  } else {
    f <- utils::getFromNamespace("add_filter", "dbplyr")
    f(...)
  }
}

add_select <- function(...) {
  if (dbplyr_uses_ops()) {
    rlang::abort("Internal error")
  } else {
    f <- utils::getFromNamespace("add_select", "dbplyr")
    f(...)
  }
}

add_summarise <- function(...) {
  if (dbplyr_uses_ops()) {
    rlang::abort("Internal error")
  } else {
    f <- utils::getFromNamespace("add_summarise", "dbplyr")
    f(...)
  }
}

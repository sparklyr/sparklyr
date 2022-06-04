add_op_single <- function(...) {
  if (dbplyr_uses_ops()) {
    f <- utils::getFromNamespace("add_op_single", "dbplyr")
    f(...)
  }
}

op_single <- function(...) {
  if (dbplyr_uses_ops()) {
    f <- utils::getFromNamespace("op_single", "dbplyr")
    f(...)
  }
}

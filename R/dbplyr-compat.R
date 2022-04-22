add_op_single <- function(...) {
  if (dbplyr_uses_ops()) {
    f <- utils::getFromNamespace("add_op_single", "dbplyr")
    f(...)
  }
}

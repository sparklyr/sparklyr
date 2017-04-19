register_s3_method <- function(pkg, generic, class, fun = NULL) {
  stopifnot(is.character(pkg), length(pkg) == 1)
  envir <- asNamespace(pkg)

  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)
  if (is.null(fun)) {
    fun <- get(paste0(generic, class), envir = parent.frame())
  } else {
    stopifnot(is.function(fun))
  }

  if (pkg %in% loadedNamespaces()) {
    registerS3method(generic, class, fun, envir = envir)
  }

  # always register hook in case package is later unloaded & reloaded
  setHook(
    packageEvent(pkg, "onLoad"),
    function(...) {
      registerS3method(generic, class, fun, envir = envir)
    }
  )
}

register_dbplyr_method <- function(name, ...) {
  functionRef <- get(name, envir = asNamespace("dbplyr"))
  assign(name, functionRef, envir = asNamespace("sparklyr"))
}

register_dplyr_all <- function() {
  dbplyrPackage <- if (utils::packageVersion("dplyr") > "0.5.0") "dbplyr" else "dplyr"

  if (utils::packageVersion("dplyr") > "0.5.0") {
    register_dbplyr_method("add_op_single")
    register_dbplyr_method("build_sql")
    register_dbplyr_method("escape")
    register_dbplyr_method("select_query")
    register_dbplyr_method("sql_build")
    register_dbplyr_method("sql_quote")
    register_dbplyr_method("sql_render")
    register_dbplyr_method("src_sql")
    register_dbplyr_method("tbl_sql")
    register_dbplyr_method("sql_vector")
    register_dbplyr_method("sql_translator")
    register_dbplyr_method("base_agg")
    register_dbplyr_method("base_win")
    register_dbplyr_method("named_commas")
    register_dbplyr_method("base_scalar")
    register_dbplyr_method("op_vars")
    register_dbplyr_method("sql_variant")
    register_dbplyr_method("sql_prefix")
  }
  else {
    register_s3_method(dbplyrPackage, "src_desc", "src_spark", fun = src_desc.src_spark)
  }

  register_s3_method(dbplyrPackage, "sql_build", "op_sample_frac", fun = sql_build.op_sample_frac)
  register_s3_method(dbplyrPackage, "sql_build", "op_sample_n", fun = sql_build.op_sample_n)
  register_s3_method(dbplyrPackage, "sql_build", "tbl_spark", fun = sql_build.tbl_spark)
}

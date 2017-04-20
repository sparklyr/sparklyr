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

register_dplyr_method <- function(package, name, ...) {
  functionRef <- get(name, envir = asNamespace(package))
  assign(name, functionRef, envir = asNamespace("sparklyr"))
}

register_dplyr_all <- function() {
  dbplyrPackage <- if (utils::packageVersion("dplyr") > "0.5.0") "dbplyr" else "dplyr"

  if (utils::packageVersion("dplyr") > "0.5.0") {
    register_dplyr_method("dbplyr", "add_op_single")
    register_dplyr_method("dbplyr", "build_sql")
    register_dplyr_method("dbplyr", "escape")
    register_dplyr_method("dbplyr", "select_query")
    register_dplyr_method("dbplyr", "sql_build")
    register_dplyr_method("dbplyr", "sql_quote")
    register_dplyr_method("dbplyr", "sql_render")
    register_dplyr_method("dbplyr", "src_sql")
    register_dplyr_method("dbplyr", "tbl_sql")
    register_dplyr_method("dbplyr", "sql_vector")
    register_dplyr_method("dbplyr", "sql_translator")
    register_dplyr_method("dbplyr", "base_agg")
    register_dplyr_method("dbplyr", "base_win")
    register_dplyr_method("dbplyr", "named_commas")
    register_dplyr_method("dbplyr", "base_scalar")
    register_dplyr_method("dbplyr", "op_vars")
    register_dplyr_method("dbplyr", "sql_variant")
    register_dplyr_method("dbplyr", "sql_prefix")

    register_dplyr_method("dplyr", "db_desc")
  }
  else {
    register_dplyr_method("dplyr", "add_op_single")
    register_dplyr_method("dplyr", "build_sql")
    register_dplyr_method("dplyr", "escape")
    register_dplyr_method("dplyr", "select_query")
    register_dplyr_method("dplyr", "sql_build")
    register_dplyr_method("dplyr", "sql_quote")
    register_dplyr_method("dplyr", "sql_render")
    register_dplyr_method("dplyr", "src_sql")
    register_dplyr_method("dplyr", "tbl_sql")
    register_dplyr_method("dplyr", "sql_vector")
    register_dplyr_method("dplyr", "sql_translator")
    register_dplyr_method("dplyr", "base_agg")
    register_dplyr_method("dplyr", "base_win")
    register_dplyr_method("dplyr", "named_commas")
    register_dplyr_method("dplyr", "base_scalar")
    register_dplyr_method("dplyr", "op_vars")
    register_dplyr_method("dplyr", "sql_variant")
    register_dplyr_method("dplyr", "sql_prefix")

    register_s3_method(dbplyrPackage, "src_desc", "src_spark", fun = src_desc.src_spark)
  }

  register_s3_method(dbplyrPackage, "sql_build", "op_sample_frac", fun = sql_build.op_sample_frac)
  register_s3_method(dbplyrPackage, "sql_build", "op_sample_n", fun = sql_build.op_sample_n)
  register_s3_method(dbplyrPackage, "sql_build", "tbl_spark", fun = sql_build.tbl_spark)
}

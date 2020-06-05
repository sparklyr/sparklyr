#' Register a Prallel Backend
#'
#' Registers a parallel backend using the \code{foreach} package.
#'
#' @param spark_conn spark connection to use
#' @param ... additional options for sparklyr parallel backend
#'            (currently only the only valid option is nocompile = {T, F})
#'
#' @return None
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#' registerDoSpark(sc, nocompile = FALSE)
#' }
#'
#' @export
registerDoSpark <- function(spark_conn, ...) {
  # internal function to process registerDoSpark options
  .processOpts <- function(...) {
    opts <- list(...)
    optnames <- names(opts)
    if (is.null(optnames))
      optnames <- rep('', length(opts))

    # filter out unnamed arguments with a warning
    unnamed <- ! nzchar(optnames)
    if (any(unnamed)) {
      warning('ignoring doSpark package option(s) specified with unnamed argument')
      opts <- opts[!unnamed]
      optnames <- optnames[!unnamed]
    }

    # filter out unrecognized options with a warning
    recog <- optnames %in% .globals$do_spark$valid_options
    if (any(!recog)) {
      warning(sprintf('ignoring unrecognized doSpark package option(s): %s',
                      paste(optnames[!recog], collapse = ', ')), call. = FALSE)
      opts <- opts[recog]
      optnames <- optnames[recog]
    }

    # clear .options from any previous call to registerDoSpark
    remove(list = ls(.globals$do_spark$options), pos = .globals$do_spark$options)

    # set new options
    for (i in seq(along=opts)) {
      assign(optnames[i], opts[[i]], pos=.globals$do_spark$options)
    }
  }

  # internal function called by foreach
  .doSpark <- function(obj, expr, envir, data) {
    obj$packages <- unique(c(obj$packages, (.packages())))
    # internal function to compile an expression if possible
    .compile <- function(expr, ...) {
      if (getRversion() < "2.13.0" || isTRUE(.globals$do_spark$options$nocompile))
        expr
      else
        compiler::compile(expr, ...)
    }

    # internal functions to serialize and unserialize an arbitrary R object to/from string
    #
    # NOTE: here we are (reasonably) assuming foreach items are not astronomical in size and
    # therefore the ~33% space overhead from base64 encode is still acceptable.
    # If this assumption were not true, then base64 would need to be replaced with another
    # more efficient binary-to-text encoding such as yEnc (which has only 1-2% space overhead).
    .encode_item <- function(item) list(encoded = base64enc::base64encode(serialize(item, NULL)))
    .decode_item <- function(item) unserialize(base64enc::base64decode(item))

    expr_globals <- globals::globalsOf(expr, envir = envir, recursive = TRUE, mustExist = FALSE)

    # internal function to process spark data frames
    .process_spark_items <- function(...) {
      # load necessary packages
      for (p in obj$packages) library(p, character.only=TRUE)

      f <- function(item) {
        enclos <- envir
        expr_globals <- as.list(expr_globals)
        for (k in names(expr_globals)) {
          v <- expr_globals$k
          if (identical(typeof(v), "closure")) {
            # to make `enclos` truly self-contained, each of its element that is
            # a closure will need to have its enclosing environment replaced with
            # `enclos` itself
            environment(v) <- enclos
          }
          if (!exists(k, where = enclos)) {
            assign(k, v, pos = enclos)
          }
        }
        # `enclos` now contains a snapshot of all external variables and functions
        # needed for evaluating `expr`
        tryCatch(
          {
            res <- eval(
              expr,
              envir = as.list(.decode_item(item$encoded)),
              enclos = enclos
            )
            .encode_item(res)
          },
          error = function(ex) {
            .encode_item(ex)
          }
        )
      }
      encoded_res <- sdf_collect(spark_items %>% spark_apply(f, ...))[[1]]
      lapply(encoded_res, .decode_item)
    }

    if (!inherits(obj, "foreach"))
      stop("obj must be a foreach object")

    spark_conn <- data$spark_conn
    spark_apply_args <- data$spark_apply_args

    it <- iterators::iter(obj)
    accumulator <- foreach::makeAccum(it)
    items <- it %>% as.list() %>% lapply(.encode_item)
    spark_items <- sdf_copy_to(spark_conn, items, overwrite = TRUE)
    expr <- .compile(expr)
    res <- do.call(.process_spark_items, spark_apply_args)
    tryCatch(
      accumulator(res, seq(along = res)),
      error = function(e) {
        cat("error calling combine function:\n", file=stderr())
        print(e)
      }
    )
    err_val <- foreach::getErrorValue(it)
    err_idx <- foreach::getErrorIndex(it)
    if (identical(obj$errorHandling, "stop") && !is.null(err_val)) {
      msg <- sprintf("task %d failed - \"%s\"", err_idx, conditionMessage(err_val))
      stop(simpleError(msg, call = expr))
    } else {
      foreach::getResult(it)
    }
  }

  # internal function reporting info about the sparklyr parallel backend
  .info <- function(data, item) {
    pkgName <- "doSpark"
    switch(item,
      name = pkgName,
      version = packageDescription(pkgName, fields = "Version"),
      workers = tryCatch(
        {
          spark_conf<-invoke(data$spark_conn$state$spark_context, "getConf")
          # return an integer value greater than 1 as number of workers if
          # "spark.executor.instances" is not set
          invoke(spark_conf, "getInt", "spark.executor.instances", as.integer(2))
        },
        # return 0 as number of workers if there is an exception
        error = function(e) { 0 }
      ),
      NULL
    )
  }

  .globals$do_spark <- list(
    # list of valid options for doSpark
    valid_options = c('nocompile'),
    # options from the last successful call to registerDoSpark
    options = new.env(parent=emptyenv())
  )

  .processOpts(...)
  foreach::setDoPar(
    fun=.doSpark,
    data=list(spark_conn = spark_conn, spark_apply_args = list(...)),
    info=.info
  )
}

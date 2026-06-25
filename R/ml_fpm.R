#' Frequent Pattern Mining -- FPGrowth
#'
#' A parallel FP-growth algorithm to mine frequent itemsets.
#'
#' @template roxlate-ml-x
#' @param items_col Items column name. Default: "items"
#' @param min_confidence Minimal confidence for generating Association Rule.
#'   \code{min_confidence} will not affect the mining for frequent itemsets, but
#'   will affect the association rules generation. Default: 0.8
#' @param min_support Minimal support level of the frequent pattern. [0.0, 1.0].
#'   Any pattern that appears more than (min_support * size-of-the-dataset) times
#'    will be output in the frequent itemsets. Default: 0.3
#' @template roxlate-ml-prediction-col
#' @template roxlate-ml-uid
#' @template roxlate-ml-dots
#' @name ml_fpgrowth
#' @export
ml_fpgrowth <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_fpgrowth")
}

#' @export
ml_fpgrowth.spark_connection <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"),
  ...
) {
  .args <- list(
    items_col = items_col,
    min_confidence = min_confidence,
    min_support = min_support,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_fpgrowth()

  uid <- cast_string(uid)
  jobj <- invoke_new(x, "org.apache.spark.ml.fpm.FPGrowth", uid) %>%
    invoke(
      "%>%",
      list("setItemsCol", .args[["items_col"]]),
      list("setMinConfidence", .args[["min_confidence"]]),
      list("setMinSupport", .args[["min_support"]]),
      list("setPredictionCol", .args[["prediction_col"]])
    )

  new_ml_fpgrowth(jobj)
}

#' @export
ml_fpgrowth.ml_pipeline <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"),
  ...
) {
  stage <- ml_fpgrowth.spark_connection(
    x = spark_connection(x),
    items_col = items_col,
    min_confidence = min_confidence,
    min_support = min_support,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_fpgrowth.tbl_spark <- function(
  x,
  items_col = "items",
  min_confidence = 0.8,
  min_support = 0.3,
  prediction_col = "prediction",
  uid = random_string("fpgrowth_"),
  ...
) {
  stage <- ml_fpgrowth.spark_connection(
    x = spark_connection(x),
    items_col = items_col,
    min_confidence = min_confidence,
    min_support = min_support,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )

  stage %>%
    ml_fit(x)
}

# Validator
validator_ml_fpgrowth <- function(.args) {
  .args[["items_col"]] <- cast_string(.args[["items_col"]])
  .args[["min_confidence"]] <- cast_scalar_double(.args[["min_confidence"]])
  .args[["min_support"]] <- cast_scalar_double(.args[["min_support"]])
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args
}

new_ml_fpgrowth <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_fpgrowth")
}

new_ml_fpgrowth_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    # def
    association_rules = function() {
      invoke(jobj, "associationRules") %>%
        sdf_register()
    },
    freq_itemsets = invoke(jobj, "freqItemsets") %>%
      sdf_register(),
    class = "ml_fpgrowth_model"
  )
}

#' @rdname ml_fpgrowth
#' @param model A fitted FPGrowth model returned by \code{ml_fpgrowth()}
#' @export
ml_association_rules <- function(model) {
  model$association_rules()
}

#' @rdname ml_fpgrowth
#' @export
ml_freq_itemsets <- function(model) {
  model$freq_itemsets
}

#' Frequent Pattern Mining -- PrefixSpan
#'
#' PrefixSpan algorithm for mining frequent itemsets.
#'
#' @template roxlate-ml-x
#' @param seq_col The name of the sequence column in dataset (defaults to
#'   "sequence"). Rows with nulls in this column are ignored.
#' @param min_support The minimum support required to be considered a frequent
#'   sequential pattern.
#' @param max_pattern_length The maximum length of a frequent sequential
#'   pattern. Any frequent pattern exceeding this length will not be included in
#'   the results.
#' @param max_local_proj_db_size The maximum number of items allowed in a
#'   prefix-projected database before local iterative processing of the
#'   projected database begins. This parameter should be tuned with respect to
#'   the size of your executors.
#' @template roxlate-ml-uid
#' @template roxlate-ml-dots
#' @name ml_prefixspan
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.4.0")
#'
#' items_df <- dplyr::tibble(
#'   seq = list(
#'     list(list(1, 2), list(3)),
#'     list(list(1), list(3, 2), list(1, 2)),
#'     list(list(1, 2), list(5)),
#'     list(list(6))
#'   )
#' )
#' items_sdf <- copy_to(sc, items_df, overwrite = TRUE)
#'
#' prefix_span_model <- ml_prefixspan(
#'   sc,
#'   seq_col = "seq",
#'   min_support = 0.5,
#'   max_pattern_length = 5,
#'   max_local_proj_db_size = 32000000
#' )
#'
#' frequent_items <- prefix_span_model$frequent_sequential_patterns(items_sdf) %>% collect()
#' }
#'
#' @export
ml_prefixspan <- function(
  x,
  seq_col = "sequence",
  min_support = 0.1,
  max_pattern_length = 10,
  max_local_proj_db_size = 32000000,
  uid = random_string("prefixspan_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_prefixspan")
}

#' @export
ml_prefixspan.spark_connection <- function(
  x,
  seq_col = "sequence",
  min_support = 0.1,
  max_pattern_length = 10,
  max_local_proj_db_size = 32000000,
  uid = random_string("prefixspan_"),
  ...
) {
  .args <- list(
    seq_col = seq_col,
    min_support = min_support,
    max_pattern_length = max_pattern_length,
    max_local_proj_db_size = max_local_proj_db_size
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_prefixspan()

  uid <- cast_string(uid)
  jobj <- invoke_new(x, "org.apache.spark.ml.fpm.PrefixSpan", uid) %>%
    invoke(
      "%>%",
      list("setSequenceCol", .args[["seq_col"]]),
      list("setMinSupport", .args[["min_support"]]),
      list("setMaxPatternLength", .args[["max_pattern_length"]]),
      list("setMaxLocalProjDBSize", .args[["max_local_proj_db_size"]])
    )

  new_ml_prefixspan_model(jobj)
}

# Validator
validator_ml_prefixspan <- function(.args) {
  .args[["seq_col"]] <- cast_string(.args[["seq_col"]])
  .args[["min_support"]] <- cast_scalar_double(.args[["min_support"]])
  .args[["max_pattern_length"]] <- cast_integer(.args[["max_pattern_length"]])
  .args[["max_local_proj_db_size"]] <- cast_integer(.args[[
    "max_local_proj_db_size"
  ]])
  .args
}

new_ml_prefixspan_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    # def
    frequent_sequential_patterns = function(x) {
      invoke(jobj, "findFrequentSequentialPatterns", spark_dataframe(x)) %>%
        sdf_register()
    },
    class = "ml_prefixspan_model"
  )
}

#' @rdname ml_prefixspan
#'
#' @param model A Prefix Span model.
#'
#' @export
ml_freq_seq_patterns <- function(model) {
  model$frequent_sequential_patterns
}

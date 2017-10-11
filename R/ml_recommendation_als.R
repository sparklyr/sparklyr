#' Spark ML -- ALS
#'
#' Perform recommendation using Alternating Least Squares (ALS) matrix factorization.
#'
#' @export
ml_als <- function(
  x,
  rating_col = "rating",
  user_col = "user",
  item_col = "item",
  rank = 10L,
  reg_param = 0.1,
  implicit_prefs = FALSE,
  alpha = 1,
  nonnegative = FALSE,
  max_iter = 10L,
  num_user_blocks = 10L,
  num_item_blocks = 10L,
  checkpoint_interval = 10L,
  cold_start_strategy = "nan",
  intermediate_storage_level = "MEMORY_AND_DISK",
  final_storage_level = "MEMORY_AND_DISK",
  uid = random_string("als_"), ...
) {
  UseMethod("ml_als")
}

#' @export
ml_als.spark_connection <- function(
  x,
  rating_col = "rating",
  user_col = "user",
  item_col = "item",
  rank = 10L,
  reg_param = 0.1,
  implicit_prefs = FALSE,
  alpha = 1,
  nonnegative = FALSE,
  max_iter = 10L,
  num_user_blocks = 10L,
  num_item_blocks = 10L,
  checkpoint_interval = 10L,
  cold_start_strategy = "nan",
  intermediate_storage_level = "MEMORY_AND_DISK",
  final_storage_level = "MEMORY_AND_DISK",
  uid = random_string("als_"), ...) {

  ml_ratify_args()

  jobj <- invoke_new(sc, "org.apache.spark.ml.recommendation.ALS", uid) %>%
    invoke("setRatingCol", rating_col) %>%
    invoke("setUserCol", user_col) %>%
    invoke("setItemCol", item_col) %>%
    invoke("setRank", rank) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setImplicitPrefs", implicit_prefs) %>%
    invoke("setAlpha", alpha) %>%
    invoke("setNonnegative", nonnegative) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setNumUserBlocks", num_user_blocks) %>%
    invoke("setNumItemBlocks", num_item_blocks) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setIntermediateStorageLevel", intermediate_storage_level) %>%
    invoke("setFinalStorageLevel", final_storage_level)

  if (spark_version(x) >= "2.2.0")
    jobj <- jobj %>%
      invoke("setColdStartStrategy", cold_start_strategy)

  new_ml_als(jobj)
}

#' @export
ml_als.ml_pipeline <- function(
  x,
  rating_col = "rating",
  user_col = "user",
  item_col = "item",
  rank = 10L,
  reg_param = 0.1,
  implicit_prefs = FALSE,
  alpha = 1,
  nonnegative = FALSE,
  max_iter = 10L,
  num_user_blocks = 10L,
  num_item_blocks = 10L,
  checkpoint_interval = 10L,
  cold_start_strategy = "nan",
  intermediate_storage_level = "MEMORY_AND_DISK",
  final_storage_level = "MEMORY_AND_DISK",
  uid = random_string("als_"), ...) {

  estimator <- ml_new_stage_modified_args()
  ml_add_stage(x, estimator)
}

#' @export
ml_als.tbl_spark <- function(
  x,
  rating_col = "rating",
  user_col = "user",
  item_col = "item",
  rank = 10L,
  reg_param = 0.1,
  implicit_prefs = FALSE,
  alpha = 1,
  nonnegative = FALSE,
  max_iter = 10L,
  num_user_blocks = 10L,
  num_item_blocks = 10L,
  checkpoint_interval = 10L,
  cold_start_strategy = "nan",
  intermediate_storage_level = "MEMORY_AND_DISK",
  final_storage_level = "MEMORY_AND_DISK",
  uid = random_string("als_"), ...) {

  estimator <- ml_new_stage_modified_args()

  estimator %>%
    ml_fit(x)

}

# Validator
ml_validator_als <- function(args, nms) {
  old_new_mapping <- list(
     rating.column = "rating_col",
     user.column = "user_col",
     item.column = "item_col",
     regularization.parameter = "reg_param",
     implicit.preferences = "implicit_prefs",
     iter.max = "max_iter"
    )

  args %>%
    ml_validate_args({
      rating_col <- ensure_scalar_character(rating_col)
      user_col <- ensure_scalar_character(user_col)
      item_col <- ensure_scalar_character(item_col)
      rank <- ensure_scalar_integer(rank)
      reg_param <- ensure_scalar_double(reg_param)
      implicit_prefs <- ensure_scalar_boolean(implicit_prefs)
      alpha <- ensure_scalar_double(alpha)
      nonnegative <- ensure_scalar_boolean(nonnegative)
      max_iter <- ensure_scalar_integer(max_iter)
      num_user_blocks <- ensure_scalar_integer(num_user_blocks)
      num_item_blocks <- ensure_scalar_integer(num_item_blocks)
      checkpoint_interval <- ensure_scalar_integer(checkpoint_interval)
      cold_start_strategy <- rlang::arg_match(cold_start_strategy,
                                              c("nan", "drop"))
      intermediate_storage_level <- ensure_scalar_character(intermediate_storage_level)
      final_storage_level <- ensure_scalar_character(final_storage_level)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_als <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_als")
}

new_ml_als_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    rank = invoke(jobj, "rank"),
    recommend_for_all_items = function(num_users) {
      num_users <- ensure_scalar_integer(num_users)
      invoke(jobj, "recommendForAllItems", num_users) %>%
        sdf_register()
    },
    recommend_for_all_users = function(num_items) {
      num_items <- ensure_scalar_integer(num_items)
      invoke(jobj, "recommendForAllUsers", num_items) %>%
        sdf_register()
    },
    item_factors = invoke(jobj, "itemFactors") %>%
      sdf_register() %>%
      sdf_separate_column("features"),
    user_factors = invoke(jobj, "userFactors") %>%
      sdf_register() %>%
      sdf_separate_column("features"),
    user_col = invoke(jobj, "getUserCol"),
    item_col = invoke(jobj, "getItemCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    subclass = "ml_als_model")
}

# Generic implementations

#' @export
ml_fit.ml_als <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_als_model(jobj)
}

# Helpers

#' @rdname ml_als
#' @param model An ALS model object
#' @param type What to recommend, one of \code{items} or \code{users}
#' @param n Maximum number of recommendations to return
#'
#' @details \code{ml_recommend()} returns the top \code{n} users/items recommended for each item/user, for all items/users. The output has been transformed (exploded and separated) from the default Spark outputs to be more user friendly.
#'
#' @export
ml_recommend <- function(model, type = c("items", "users"), n = 1) {
  if (spark_version(spark_connection(model)) < "2.2.0")
    stop("'ml_recommend()' is only support for Spark 2.2+")

  type <- match.arg(type)
  n <- ensure_scalar_integer(n)
  (switch(type,
          items = model$recommend_for_all_users,
          users = model$recommend_for_all_items))(n) %>%
    mutate(recommendations = explode(!!as.name("recommendations"))) %>%
    sdf_separate_column("recommendations")
}

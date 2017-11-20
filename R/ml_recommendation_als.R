#' Spark ML -- ALS
#'
#' Perform recommendation using Alternating Least Squares (ALS) matrix factorization.
#'
#' @template roxlate-ml-x
#' @param rating_col Column name for ratings. Default: "rating"
#' @param user_col Column name for user ids. Ids must be integers. Other numeric types are supported for this column, but will be cast to integers as long as they fall within the integer value range. Default: "user"
#' @param item_col Column name for item ids. Ids must be integers. Other numeric types are supported for this column, but will be cast to integers as long as they fall within the integer value range. Default: "item"
#' @param rank Rank of the matrix factorization (positive). Default: 10
#' @param reg_param Regularization parameter.
#' @param implicit_prefs Whether to use implicit preference. Default: FALSE.
#' @param alpha Alpha parameter in the implicit preference formulation (nonnegative).
#' @param nonnegative Whether to apply nonnegativity constraints. Default: FALSE.
#' @param max_iter Maximum number of iterations.
#' @param num_user_blocks Number of user blocks (positive). Default: 10
#' @param num_item_blocks Number of item blocks (positive). Default: 10
#' @template roxlate-ml-checkpoint-interval
#' @param cold_start_strategy (Spark 2.2.0+) Strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: - "nan": predicted value for unknown ids will be NaN. - "drop": rows in the input DataFrame containing unknown ids will be dropped from the output DataFrame containing predictions. Default: "nan".
#' @param intermediate_storage_level (Spark 2.0.0+) StorageLevel for intermediate datasets. Pass in a string representation of \code{StorageLevel}. Cannot be "NONE". Default: "MEMORY_AND_DISK".
#' @param final_storage_level (Spark 2.0.0+) StorageLevel for ALS model factors. Pass in a string representation of \code{StorageLevel}. Default: "MEMORY_AND_DISK".
#' @template roxlate-ml-uid
#' @template roxlate-ml-dots
#' @return ALS attempts to estimate the ratings matrix R as the product of two lower-rank matrices, X and Y, i.e. X * Yt = R. Typically these approximations are called 'factor' matrices. The general approach is iterative. During each iteration, one of the factor matrices is held constant, while the other is solved for using least squares. The newly-solved factor matrix is then held constant while solving for the other factor matrix.
#'
#' This is a blocked implementation of the ALS factorization algorithm that groups the two sets of factors (referred to as "users" and "products") into blocks and reduces communication by only sending one copy of each user vector to each product block on each iteration, and only for the product blocks that need that user's feature vector. This is achieved by pre-computing some information about the ratings matrix to determine the "out-links" of each user (which blocks of products it will contribute to) and "in-link" information for each product (which of the feature vectors it receives from each user block it will depend on). This allows us to send only an array of feature vectors between each user block and product block, and have the product block find the users' ratings and update the products based on these messages.
#'
#' For implicit preference data, the algorithm used is based on "Collaborative Filtering for Implicit Feedback Datasets", available at \url{http://dx.doi.org/10.1109/ICDM.2008.22}, adapted for the blocked approach used here.
#'
#' Essentially instead of finding the low-rank approximations to the rating matrix R, this finds the approximations for a preference matrix P where the elements of P are 1 if r is greater than 0 and 0 if r is less than or equal to 0. The ratings then act as 'confidence' values related to strength of indicated user preferences rather than explicit ratings given to items.
#'
#' The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_als} recommender object, which is an Estimator.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the recommender appended to the pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a recommender
#'   estimator is constructed then immediately fit with the input
#'   \code{tbl_spark}, returning a recommendation model, i.e. \code{ml_als_model}.
#' }
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

  jobj <- invoke_new(x, "org.apache.spark.ml.recommendation.ALS", uid) %>%
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
    jobj_set_param("setIntermediateStorageLevel", intermediate_storage_level,
                   "MEMORY_AND_DISK", "2.0.0") %>%
    jobj_set_param("setFinalStorageLevel", final_storage_level,
                   "MEMORY_AND_DISK", "2.0.0") %>%
    jobj_set_param("setColdStartStrategy", cold_start_strategy,
                   "nan", "2.2.0")

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

# Helpers

# Hideous hack
utils::globalVariables("explode")

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

#' @rdname ml_als
#' @details \code{ml_als_factorization()} is an alias for \code{ml_als()} for backwards compatibility.
#' @export
ml_als_factorization <- function(
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

  UseMethod("ml_als")
}

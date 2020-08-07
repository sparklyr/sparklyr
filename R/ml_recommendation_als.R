#' Spark ML -- ALS
#'
#' Perform recommendation using Alternating Least Squares (ALS) matrix factorization.
#'
#' @template roxlate-ml-x
#' @param formula Used when \code{x} is a \code{tbl_spark}. R formula as a character string or a formula.
#' This is used to transform the input dataframe before fitting, see \link{ft_r_formula} for details.
#' The ALS model requires a specific formula format, please use \code{rating_col ~ user_col + item_col}.
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
#' For implicit preference data, the algorithm used is based on "Collaborative Filtering for Implicit Feedback Datasets", available at \url{https://doi.org/10.1109/ICDM.2008.22}, adapted for the blocked approach used here.
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
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#'
#' movies <- data.frame(
#'   user   = c(1, 2, 0, 1, 2, 0),
#'   item   = c(1, 1, 1, 2, 2, 0),
#'   rating = c(3, 1, 2, 4, 5, 4)
#' )
#' movies_tbl <- sdf_copy_to(sc, movies)
#'
#' model <- ml_als(movies_tbl, rating ~ user + item)
#'
#' ml_predict(model, movies_tbl)
#'
#' ml_recommend(model, type = "item", 1)
#' }
#'
#' @export
ml_als <- function(x, formula = NULL, rating_col = "rating", user_col = "user", item_col = "item",
                   rank = 10, reg_param = 0.1, implicit_prefs = FALSE, alpha = 1,
                   nonnegative = FALSE, max_iter = 10, num_user_blocks = 10,
                   num_item_blocks = 10, checkpoint_interval = 10,
                   cold_start_strategy = "nan", intermediate_storage_level = "MEMORY_AND_DISK",
                   final_storage_level = "MEMORY_AND_DISK", uid = random_string("als_"), ...) {
  check_dots_used()
  UseMethod("ml_als")
}

#' @export
ml_als.spark_connection <- function(x, formula = NULL, rating_col = "rating", user_col = "user", item_col = "item",
                                    rank = 10, reg_param = 0.1, implicit_prefs = FALSE, alpha = 1,
                                    nonnegative = FALSE, max_iter = 10, num_user_blocks = 10,
                                    num_item_blocks = 10, checkpoint_interval = 10,
                                    cold_start_strategy = "nan", intermediate_storage_level = "MEMORY_AND_DISK",
                                    final_storage_level = "MEMORY_AND_DISK", uid = random_string("als_"), ...) {
  .args <- list(
    rating_col = rating_col,
    user_col = user_col,
    item_col = item_col,
    rank = rank,
    reg_param = reg_param,
    implicit_prefs = implicit_prefs,
    alpha = alpha,
    nonnegative = nonnegative,
    max_iter = max_iter,
    num_user_blocks = num_user_blocks,
    num_item_blocks = num_item_blocks,
    checkpoint_interval = checkpoint_interval,
    cold_start_strategy = cold_start_strategy,
    intermediate_storage_level = intermediate_storage_level,
    final_storage_level = final_storage_level
  ) %>%
    validator_ml_als()

  jobj <- invoke_new(x, "org.apache.spark.ml.recommendation.ALS", uid) %>%
    (
      function(obj) {
        do.call(
          invoke,
          c(obj, "%>%", Filter(
            function(x) !is.null(x),
            list(
              list("setRatingCol", .args[["rating_col"]]),
              list("setUserCol", .args[["user_col"]]),
              list("setItemCol", .args[["item_col"]]),
              list("setRank", .args[["rank"]]),
              list("setRegParam", .args[["reg_param"]]),
              list("setImplicitPrefs", .args[["implicit_prefs"]]),
              list("setAlpha", .args[["alpha"]]),
              list("setNonnegative", .args[["nonnegative"]]),
              list("setMaxIter", .args[["max_iter"]]),
              list("setNumUserBlocks", .args[["num_user_blocks"]]),
              list("setNumItemBlocks", .args[["num_item_blocks"]]),
              list("setCheckpointInterval", .args[["checkpoint_interval"]]),
              jobj_set_param_helper(
                obj, "setIntermediateStorageLevel", .args[["intermediate_storage_level"]],
                "2.0.0", "MEMORY_AND_DISK"
              ),
              jobj_set_param_helper(
                obj, "setFinalStorageLevel", .args[["final_storage_level"]],
                "2.0.0", "MEMORY_AND_DISK"
              ),
              jobj_set_param_helper(
                obj, "setColdStartStrategy", .args[["cold_start_strategy"]],
                "2.2.0", "nan"
              )
            )
          ))
        )
      })

  new_ml_als(jobj)
}

#' @export
ml_als.ml_pipeline <- function(x, formula = NULL, rating_col = "rating", user_col = "user", item_col = "item",
                               rank = 10, reg_param = 0.1, implicit_prefs = FALSE, alpha = 1,
                               nonnegative = FALSE, max_iter = 10, num_user_blocks = 10,
                               num_item_blocks = 10, checkpoint_interval = 10,
                               cold_start_strategy = "nan", intermediate_storage_level = "MEMORY_AND_DISK",
                               final_storage_level = "MEMORY_AND_DISK", uid = random_string("als_"), ...) {
  stage <- ml_als.spark_connection(
    x = spark_connection(x),
    formula = formula,
    rating_col = rating_col,
    user_col = user_col,
    item_col = item_col,
    rank = rank,
    reg_param = reg_param,
    implicit_prefs = implicit_prefs,
    alpha = alpha,
    nonnegative = nonnegative,
    max_iter = max_iter,
    num_user_blocks = num_user_blocks,
    num_item_blocks = num_item_blocks,
    checkpoint_interval = checkpoint_interval,
    cold_start_strategy = cold_start_strategy,
    intermediate_storage_level = intermediate_storage_level,
    final_storage_level = final_storage_level,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_als.tbl_spark <- function(x, formula = NULL, rating_col = "rating", user_col = "user", item_col = "item",
                             rank = 10, reg_param = 0.1, implicit_prefs = FALSE, alpha = 1,
                             nonnegative = FALSE, max_iter = 10, num_user_blocks = 10,
                             num_item_blocks = 10, checkpoint_interval = 10,
                             cold_start_strategy = "nan", intermediate_storage_level = "MEMORY_AND_DISK",
                             final_storage_level = "MEMORY_AND_DISK", uid = random_string("als_"), ...) {
  formula <- ml_standardize_formula(formula)

  stage <- ml_als.spark_connection(
    x = spark_connection(x),
    formula = formula,
    rating_col = rating_col,
    user_col = user_col,
    item_col = item_col,
    rank = rank,
    reg_param = reg_param,
    implicit_prefs = implicit_prefs,
    alpha = alpha,
    nonnegative = nonnegative,
    max_iter = max_iter,
    num_user_blocks = num_user_blocks,
    num_item_blocks = num_item_blocks,
    checkpoint_interval = checkpoint_interval,
    cold_start_strategy = cold_start_strategy,
    intermediate_storage_level = intermediate_storage_level,
    final_storage_level = final_storage_level,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    model_als <- stage %>%
      ml_fit(x)
  } else {
    ml_construct_model_recommendation(
      new_ml_model_als,
      predictor = stage,
      formula = formula,
      dataset = x
    )
  }
}

# Validator
validator_ml_als <- function(.args) {
  .args[["rating_col"]] <- cast_string(.args[["rating_col"]])
  .args[["user_col"]] <- cast_string(.args[["user_col"]])
  .args[["item_col"]] <- cast_string(.args[["item_col"]])
  .args[["rank"]] <- cast_scalar_integer(.args[["rank"]])
  .args[["reg_param"]] <- cast_scalar_double(.args[["reg_param"]])
  .args[["implicit_prefs"]] <- cast_scalar_logical(.args[["implicit_prefs"]])
  .args[["alpha"]] <- cast_scalar_double(.args[["alpha"]])
  .args[["nonnegative"]] <- cast_scalar_logical(.args[["nonnegative"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["num_user_blocks"]] <- cast_scalar_integer(.args[["num_user_blocks"]])
  .args[["num_item_blocks"]] <- cast_scalar_integer(.args[["num_item_blocks"]])
  .args[["checkpoint_interval"]] <- cast_scalar_integer(.args[["checkpoint_interval"]])
  .args[["cold_start_strategy"]] <- cast_choice(.args[["cold_start_strategy"]], c("nan", "drop"))
  .args[["intermediate_storage_level"]] <- cast_string(.args[["intermediate_storage_level"]])
  .args[["final_storage_level"]] <- cast_string(.args[["final_storage_level"]])
  .args
}

# Constructors

new_ml_als <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_als")
}

new_ml_als_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    rank = invoke(jobj, "rank"),
    recommend_for_all_items = function(num_users) {
      num_users <- cast_scalar_integer(num_users)
      invoke(jobj, "recommendForAllItems", num_users) %>%
        sdf_register()
    },
    recommend_for_all_users = function(num_items) {
      num_items <- cast_scalar_integer(num_items)
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
    class = "ml_als_model"
  )
}

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
  version <- spark_jobj(model) %>%
    spark_connection() %>%
    spark_version()

  if (version < "2.2.0") stop("`ml_recommend()`` is only supported for Spark 2.2+.", call. = FALSE)

  model <- if (inherits(model, "ml_model_als")) model$model else model

  type <- match.arg(type)
  n <- cast_scalar_integer(n)
  (switch(type,
    items = model$recommend_for_all_users,
    users = model$recommend_for_all_items
  ))(n) %>%
    dplyr::mutate(recommendations = explode(!!as.name("recommendations"))) %>%
    sdf_separate_column("recommendations")
}

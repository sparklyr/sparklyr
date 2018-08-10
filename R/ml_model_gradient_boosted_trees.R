#' Spark ML -- Gradient Boosted Trees
#'
#' Perform binary classification and regression using gradient boosted trees. Multiclass classification is not supported yet.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-feature-subset-strategy
#' @param max_iter Maxmimum number of iterations.
#' @param step_size Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator. (default = 0.1)
#' @param subsampling_rate Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
#' @param loss_type Loss function which GBT tries to minimize. Supported: \code{"squared"} (L2) and \code{"absolute"} (L1) (default = squared) for regression and \code{"logistic"} (default) for classification. For \code{ml_gradient_boosted_trees}, setting \code{"auto"}
#'   will default to the appropriate loss type based on model type.
#' @name ml_gradient_boosted_trees
NULL

#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_gradient_boosted_trees} is a wrapper around \code{ml_gbt_regressor.tbl_spark} and \code{ml_gbt_classifier.tbl_spark} and calls the appropriate method based on model type.
#' @template roxlate-ml-old-feature-response
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' gbt_model <- iris_training %>%
#'   ml_gradient_boosted_trees(Sepal_Length ~ Petal_Length + Petal_Width)
#'
#' pred <- sdf_predict(iris_test, gbt_model)
#'
#' ml_regression_evaluator(pred, label_col = "Sepal_Length")
#' }
#'
#' @export
ml_gradient_boosted_trees <- function(x, formula = NULL,
                                      type = c("auto", "regression", "classification"),
                                      features_col = "features", label_col = "label",
                                      prediction_col = "prediction", probability_col = "probability",
                                      raw_prediction_col = "rawPrediction", checkpoint_interval = 10,
                                      loss_type = c("auto", "logistic", "squared", "absolute"),
                                      max_bins = 32, max_depth = 5, max_iter = 20L,
                                      min_info_gain = 0, min_instances_per_node = 1,
                                      step_size = 0.1, subsampling_rate = 1, feature_subset_strategy = "auto",
                                      seed = NULL, thresholds = NULL, cache_node_ids = FALSE,
                                      max_memory_in_mb = 256, uid = random_string("gradient_boosted_trees_"),
                                      response = NULL, features = NULL, ...) {
  ml_formula_transformation()
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  response_type <- schema[[response_col]]$type

  type <- rlang::arg_match(type)
  model_type <- if (!identical(type, "auto")) type else {
    if (response_type %in% c("DoubleType", "IntegerType"))
      "regression"
    else
      "classification"
  }

  loss_type <- rlang::arg_match(loss_type)
  loss_type <- if (identical(loss_type, "auto")) {
    if (identical(model_type, "classification")) "logistic" else "squared"
  } else if (identical(model_type, "regression")) {
    if (!loss_type %in% c("squared", "absolute"))
      stop("`loss_type` must be \"squared\" or \"absolute\" for regression.")
    loss_type
  } else {
    if (!identical(loss_type, "logistic"))
      stop("`loss_type` must be \"logistic\" for classification.")
    loss_type
  }

  if (spark_version(spark_connection(x)) < "2.2.0" && !is.null(thresholds))
    stop("`thresholds` is only supported for GBT in Spark 2.2.0+.")

  routine <- switch(model_type,
                    regression = ml_gbt_regressor,
                    classification = ml_gbt_classifier)

  args <- c(as.list(environment()), list(...))
  args$response <- NULL
  args$features <- NULL
  do.call(routine, args)
}

new_ml_model_gbt_classification <- function(pipeline, pipeline_model, model,
                                            dataset, formula, feature_names,
                                            index_labels, call) {
  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_gbt_classification",
    .features = feature_names,
    .index_labels = index_labels
  )
}

new_ml_model_gbt_regression <- function(pipeline, pipeline_model, model,
                                        dataset, formula, feature_names, call) {
  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_gbt_regression",
    .features = feature_names
  )
}

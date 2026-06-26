#' Feature Transformation -- StandardScaler (Estimator)
#'
#' Standardizes features by removing the mean and scaling to unit variance using
#'   column summary statistics on the samples in the training set. The "unit std"
#'    is computed using the corrected sample standard deviation, which is computed
#'    as the square root of the unbiased sample variance.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param with_mean Whether to center the data with mean before scaling. It will
#'   build a dense output, so take care when applying to sparse input. Default: FALSE
#' @param with_std Whether to scale the data to unit standard deviation. Default: TRUE
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' features <- c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")
#'
#' iris_tbl %>%
#'   ft_vector_assembler(
#'     input_col = features,
#'     output_col = "features_temp"
#'   ) %>%
#'   ft_standard_scaler(
#'     input_col = "features_temp",
#'     output_col = "features",
#'     with_mean = TRUE
#'   )
#' }
#'
#' @export
ft_standard_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  with_mean = FALSE,
  with_std = TRUE,
  uid = random_string("standard_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_standard_scaler")
}

ml_standard_scaler <- ft_standard_scaler

#' @export
ft_standard_scaler.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  with_mean = FALSE,
  with_std = TRUE,
  uid = random_string("standard_scaler_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    with_mean = with_mean,
    with_std = with_std,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_standard_scaler()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.StandardScaler",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke(
      "%>%",
      list("setWithMean", .args[["with_mean"]]),
      list("setWithStd", .args[["with_std"]])
    ) %>%
    new_ml_standard_scaler()

  estimator
}

#' @export
ft_standard_scaler.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  with_mean = FALSE,
  with_std = TRUE,
  uid = random_string("standard_scaler_"),
  ...
) {
  stage <- ft_standard_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    with_mean = with_mean,
    with_std = with_std,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_standard_scaler.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  with_mean = FALSE,
  with_std = TRUE,
  uid = random_string("standard_scaler_"),
  ...
) {
  stage <- ft_standard_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    with_mean = with_mean,
    with_std = with_std,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_standard_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_standard_scaler")
}

new_ml_standard_scaler_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    mean = possibly_null(read_spark_vector)(jobj, "mean"),
    std = possibly_null(read_spark_vector)(jobj, "std"),
    class = "ml_standard_scaler_model"
  )
}

validator_ml_standard_scaler <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["with_mean"]] <- cast_scalar_logical(.args[["with_mean"]])
  .args[["with_std"]] <- cast_scalar_logical(.args[["with_std"]])
  .args
}

#' Feature Transformation -- MinMaxScaler (Estimator)
#'
#' Rescale each feature individually to a common range [min, max] linearly using
#'   column summary statistics, which is also known as min-max normalization or
#'   Rescaling
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param max Upper bound after transformation, shared by all features Default: 1.0
#' @param min Lower bound after transformation, shared by all features Default: 0.0
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' features <- c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")
#'
#' iris_tbl %>%
#'   ft_vector_assembler(
#'     input_col = features,
#'     output_col = "features_temp"
#'   ) %>%
#'   ft_min_max_scaler(
#'     input_col = "features_temp",
#'     output_col = "features"
#'   )
#' }
#'
#' @export
ft_min_max_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min = 0,
  max = 1,
  uid = random_string("min_max_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_min_max_scaler")
}

ml_min_max_scaler <- ft_min_max_scaler

#' @export
ft_min_max_scaler.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min = 0,
  max = 1,
  uid = random_string("min_max_scaler_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    min = min,
    max = max,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_min_max_scaler()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.MinMaxScaler",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setMin", .args[["min"]]) %>%
    invoke("setMax", .args[["max"]]) %>%
    new_ml_min_max_scaler()

  estimator
}

#' @export
ft_min_max_scaler.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min = 0,
  max = 1,
  uid = random_string("min_max_scaler_"),
  ...
) {
  stage <- ft_min_max_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min = min,
    max = max,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_min_max_scaler.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min = 0,
  max = 1,
  uid = random_string("min_max_scaler_"),
  ...
) {
  stage <- ft_min_max_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min = min,
    max = max,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_min_max_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_min_max_scaler")
}

new_ml_min_max_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_min_max_scaler_model")
}

validator_ml_min_max_scaler <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["min"]] <- cast_scalar_double(.args[["min"]])
  .args[["max"]] <- cast_scalar_double(.args[["max"]])
  .args
}

#' Feature Transformation -- MaxAbsScaler (Estimator)
#'
#' Rescale each feature individually to range [-1, 1] by dividing through the
#'   largest maximum absolute value in each feature. It does not shift/center the
#'   data, and thus does not destroy any sparsity.
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' features <- c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")
#'
#' iris_tbl %>%
#'   ft_vector_assembler(
#'     input_col = features,
#'     output_col = "features_temp"
#'   ) %>%
#'   ft_max_abs_scaler(
#'     input_col = "features_temp",
#'     output_col = "features"
#'   )
#' }
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @export
ft_max_abs_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("max_abs_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_max_abs_scaler")
}

ml_max_abs_scaler <- ft_max_abs_scaler

#' @export
ft_max_abs_scaler.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("max_abs_scaler_"),
  ...
) {
  spark_require_version(x, "2.0.0", "MaxAbsScaler")

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_max_abs_scaler()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.MaxAbsScaler",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    new_ml_max_abs_scaler()

  estimator
}

#' @export
ft_max_abs_scaler.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("max_abs_scaler_"),
  ...
) {
  stage <- ft_max_abs_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_max_abs_scaler.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("max_abs_scaler_"),
  ...
) {
  stage <- ft_max_abs_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_max_abs_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_max_abs_scaler")
}

new_ml_max_abs_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_max_abs_scaler_model")
}

validator_ml_max_abs_scaler <- function(.args) {
  validate_args_transformer(.args)
}

#' Feature Transformation -- RobustScaler (Estimator)
#'
#' RobustScaler removes the median and scales the data according to the quantile range.
#' The quantile range is by default IQR (Interquartile Range, quantile range between the
#' 1st quartile = 25th quantile and the 3rd quartile = 75th quantile) but can be configured.
#' Centering and scaling happen independently on each feature by computing the relevant
#' statistics on the samples in the training set. Median and quantile range are then
#' stored to be used on later data using the transform method.
#' Note that missing values are ignored in the computation of medians and ranges.
#'
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param lower Lower quantile to calculate quantile range.
#' @param upper Upper quantile to calculate quantile range.
#' @param with_centering Whether to center data with median.
#' @param with_scaling Whether to scale the data to quantile range.
#' @param relative_error The target relative error for quantile computation.
#'
#' @export
ft_robust_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  lower = 0.25,
  upper = 0.75,
  with_centering = TRUE,
  with_scaling = TRUE,
  relative_error = 0.001,
  uid = random_string("ft_robust_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_robust_scaler")
}

ml_robust_scaler <- ft_robust_scaler

#' @export
ft_robust_scaler.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  lower = 0.25,
  upper = 0.75,
  with_centering = TRUE,
  with_scaling = TRUE,
  relative_error = 0.001,
  uid = random_string("ft_robust_scaler_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    lower = lower,
    upper = upper,
    with_centering = with_centering,
    with_scaling = with_scaling,
    relative_error = relative_error,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_robust_scaler()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.RobustScaler",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setLower", .args[["lower"]]) %>%
    invoke("setUpper", .args[["upper"]]) %>%
    invoke("setWithCentering", .args[["with_centering"]]) %>%
    invoke("setWithScaling", .args[["with_scaling"]]) %>%
    invoke("setRelativeError", .args[["relative_error"]]) %>%
    new_ml_robust_scaler()

  estimator
}

#' @export
ft_robust_scaler.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  lower = 0.25,
  upper = 0.75,
  with_centering = TRUE,
  with_scaling = TRUE,
  relative_error = 0.001,
  uid = random_string("ft_robust_scaler_"),
  ...
) {
  stage <- ft_robust_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    lower = lower,
    upper = upper,
    with_centering = with_centering,
    with_scaling = with_scaling,
    relative_error = relative_error,
    uid = uid,
    ...
  )

  ml_add_stage(x, stage)
}

#' @export
ft_robust_scaler.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  lower = 0.25,
  upper = 0.75,
  with_centering = TRUE,
  with_scaling = TRUE,
  relative_error = 0.001,
  uid = random_string("ft_robust_scaler_"),
  ...
) {
  stage <- ft_robust_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    lower = lower,
    upper = upper,
    with_centering = with_centering,
    with_scaling = with_scaling,
    relative_error = relative_error,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_robust_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_robust_scaler")
}

new_ml_robust_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_robust_scaler")
}

validator_ml_robust_scaler <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["lower"]] <- cast_scalar_double(.args[["lower"]])
  .args[["upper"]] <- cast_scalar_double(.args[["upper"]])
  .args[["with_centering"]] <- cast_scalar_logical(.args[["with_centering"]])
  .args[["with_scaling"]] <- cast_scalar_logical(.args[["with_scaling"]])
  .args[["relative_error"]] <- cast_scalar_double(.args[["relative_error"]])
  .args
}

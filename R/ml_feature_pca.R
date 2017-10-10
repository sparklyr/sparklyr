#' Feature Tranformation -- PCA (Estimator)
#'
#' PCA trains a model to project vectors to a lower dimensional space of the top k principal components.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#'
#' @param k The number of principal components
#'
#' @export
ft_pca <- function(
  x, input_col, output_col, k, dataset = NULL,
  uid = random_string("pca_"), ...) {
  UseMethod("ft_pca")
}

#' @export
ft_pca.spark_connection <- function(
  x, input_col, output_col, k, dataset = NULL,
  uid = random_string("pca_"), ...) {

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.PCA",
                                  input_col, output_col, uid) %>%
    invoke("setK", k) %>%
    new_ml_pca()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_pca.ml_pipeline <- function(
  x, input_col, output_col, k, dataset = NULL,
  uid = random_string("pca_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_pca.tbl_spark <- function(
  x, input_col, output_col, k, dataset = NULL,
  uid = random_string("pca_"), ...
) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}
# Validator

ml_validator_pca <- function(args, nms) {
  args %>%
    ml_validate_args(
      {
        k <- ensure_scalar_integer(k)
      }) %>%
    ml_extract_args(nms)
}


# Constructors
#
new_ml_pca <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_pca")
}

new_ml_pca_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    explained_variance = try_null(read_spark_vector(jobj, "explainedVariance")),
    pc = try_null(read_spark_matrix(jobj, "pc")),
    subclass = "ml_pca_model")
}

# Generic implementations
#' @export
ml_fit.ml_pca <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_pca_model(jobj)
}

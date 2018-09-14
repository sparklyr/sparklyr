#' Feature Transformation -- PCA (Estimator)
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
ft_pca <- function(x, input_col = NULL, output_col = NULL, k = NULL, dataset = NULL,
                   uid = random_string("pca_"), ...) {
  UseMethod("ft_pca")
}

#' @export
ft_pca.spark_connection <- function(x, input_col = NULL, output_col = NULL, k = NULL, dataset = NULL,
                                    uid = random_string("pca_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    k = k,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_pca()

  estimator <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.PCA",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    maybe_set_param("setK", .args[["k"]]) %>%
    new_ml_pca()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_pca.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, k = NULL, dataset = NULL,
                               uid = random_string("pca_"), ...) {

  stage <- ft_pca.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    k = k,
    dataset = dataset,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)

}

#' @export
ft_pca.tbl_spark <- function(x, input_col = NULL, output_col = NULL, k = NULL, dataset = NULL,
                             uid = random_string("pca_"), ...) {

  stage <- ft_pca.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    k = k,
    dataset = dataset,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

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

ml_validator_pca <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["k"]] <- cast_nullable_scalar_integer(.args[["k"]])
  .args
}


#' @rdname ft_pca
#' @param features The columns to use in the principal components
#'   analysis. Defaults to all columns in \code{x}.
#' @param pc_prefix Length-one character vector used to prepend names of components.
#'
#' @details \code{ml_pca()} is a wrapper around \code{ft_pca()} that returns a
#'   \code{ml_model}.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' iris_tbl %>%
#'   select(-Species) %>%
#'   ml_pca(k = 2)
#' }
#'
#' @export
#' @importFrom dplyr tbl_vars
ml_pca <- function(x,
                   features = tbl_vars(x),
                   k = length(features),
                   pc_prefix = "PC",
                   ...)
{
  k <- cast_scalar_integer(k)

  sc <- spark_connection(x)

  assembled <- random_string("assembled")
  out <- random_string("out")

  pipeline <- ml_pipeline(sc) %>%
    ft_vector_assembler(features, assembled) %>%
    ft_pca(assembled, out, k = k)

  pipeline_model <- pipeline %>%
    ml_fit(x)

  model <- pipeline_model %>%
    ml_stage(2)

  pc <- model$pc
  pc_names <- paste0(pc_prefix, seq_len(ncol(pc)))
  rownames(pc) <- features
  colnames(pc) <- pc_names

  explained_variance <- model$explained_variance

  if (!is.null(explained_variance))
    names(explained_variance) <- pc_names

  new_ml_model(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    k = k,
    pc = pc,
    explained_variance = explained_variance,
    dataset = x,
    subclass = "ml_model_pca"
  )
}

#' @export
print.ml_model_pca <- function(x, ...) {

  cat("Explained variance:", sep = "\n")
  if (is.null(x$explained_variance)) {
    cat("[not available in this version of Spark]", sep = "\n")
  } else {
    print_newline()
    print(x$explained_variance)
  }

  print_newline()
  cat("Rotation:", sep = "\n")
  print(x$pc)
}


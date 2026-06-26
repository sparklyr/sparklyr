#' Feature Transformation -- VectorAssembler (Transformer)
#'
#' Combine multiple vectors into a single row-vector; that is,
#' where each row element of the newly generated column is a
#' vector formed by concatenating each row element from the
#' specified input columns.
#'
#' @param input_cols The names of the input columns
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_vector_assembler <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("vector_assembler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_vector_assembler")
}

ml_vector_assembler <- ft_vector_assembler

#' @export
ft_vector_assembler.spark_connection <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("vector_assembler_"),
  ...
) {
  .args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_vector_assembler()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.VectorAssembler",
    input_cols = .args[["input_cols"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  )

  new_ml_vector_assembler(jobj)
}

#' @export
ft_vector_assembler.ml_pipeline <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("vector_assembler_"),
  ...
) {
  stage <- ft_vector_assembler.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_vector_assembler.tbl_spark <- function(
  x,
  input_cols = NULL,
  output_col = NULL,
  uid = random_string("vector_assembler_"),
  ...
) {
  stage <- ft_vector_assembler.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_vector_assembler <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_vector_assembler")
}

validator_ml_vector_assembler <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args
}

#' Feature Transformation -- VectorSlicer (Transformer)
#'
#' Takes a feature vector and outputs a new feature vector with a subarray of the original features.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param indices An vector of indices to select features from a vector column.
#'   Note that the indices are 0-based.
#' @export
ft_vector_slicer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  indices = NULL,
  uid = random_string("vector_slicer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_vector_slicer")
}

ml_vector_slicer <- ft_vector_slicer

#' @export
ft_vector_slicer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  indices = NULL,
  uid = random_string("vector_slicer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    indices = indices,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_vector_slicer()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.VectorSlicer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setIndices", .args[["indices"]])

  new_ml_vector_slicer(jobj)
}

#' @export
ft_vector_slicer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  indices = NULL,
  uid = random_string("vector_slicer_"),
  ...
) {
  stage <- ft_vector_slicer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    indices = indices,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_vector_slicer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  indices = NULL,
  uid = random_string("vector_slicer_"),
  ...
) {
  stage <- ft_vector_slicer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    indices = indices,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_vector_slicer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_vector_slicer")
}

validator_ml_vector_slicer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["indices"]] <- cast_integer_list(.args[["indices"]], allow_null = TRUE)
  .args
}

#' Feature Transformation -- VectorIndexer (Estimator)
#'
#' Indexing categorical feature columns in a dataset of Vector.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#' @param max_categories Threshold for the number of values a categorical feature can take. If a feature is found to have > \code{max_categories} values, then it is declared continuous. Must be greater than or equal to 2. Defaults to 20.
#'
#' @export
ft_vector_indexer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  max_categories = 20,
  uid = random_string("vector_indexer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_vector_indexer")
}

ml_vector_indexer <- ft_vector_indexer

#' @export
ft_vector_indexer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  max_categories = 20,
  uid = random_string("vector_indexer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    handle_invalid = handle_invalid,
    max_categories = max_categories,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_vector_indexer()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.VectorIndexer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param(
      "setHandleInvalid",
      .args[["handle_invalid"]],
      "2.3.0",
      "error"
    ) %>%
    invoke("setMaxCategories", .args[["max_categories"]]) %>%
    new_ml_vector_indexer()

  estimator
}

#' @export
ft_vector_indexer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  max_categories = 20,
  uid = random_string("vector_indexer_"),
  ...
) {
  stage <- ft_vector_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    max_categories = max_categories,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_vector_indexer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  handle_invalid = "error",
  max_categories = 20,
  uid = random_string("vector_indexer_"),
  ...
) {
  stage <- ft_vector_indexer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    max_categories = max_categories,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_vector_indexer <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_vector_indexer")
}

new_ml_vector_indexer_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_vector_indexer_model")
}

validator_ml_vector_indexer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["max_categories"]] <- cast_scalar_integer(.args[["max_categories"]])
  .args
}

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
ft_pca <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  k = NULL,
  uid = random_string("pca_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_pca")
}

#' @export
ft_pca.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  k = NULL,
  uid = random_string("pca_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    k = k,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_pca()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.PCA",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setK", .args[["k"]]) %>%
    new_ml_pca()

  estimator
}

#' @export
ft_pca.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  k = NULL,
  uid = random_string("pca_"),
  ...
) {
  stage <- ft_pca.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    k = k,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_pca.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  k = NULL,
  uid = random_string("pca_"),
  ...
) {
  stage <- ft_pca.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    k = k,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_pca <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_pca")
}

new_ml_pca_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    explained_variance = possibly_null(
      ~ read_spark_vector(jobj, "explainedVariance")
    )(),
    pc = possibly_null(~ read_spark_matrix(jobj, "pc"))(),
    class = "ml_pca_model"
  )
}

validator_ml_pca <- function(.args) {
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
ml_pca <- function(
  x,
  features = tbl_vars(x),
  k = length(features),
  pc_prefix = "PC",
  ...
) {
  # If being used as a constructor alias for `ft_pca()`:
  if (inherits(x, "spark_connection")) {
    return(
      rlang::exec("ft_pca.spark_connection", !!!rlang::dots_list(x = x, ...))
    )
  }

  k <- cast_scalar_integer(k)

  sc <- spark_connection(x)

  assembled <- random_string("assembled")
  out <- random_string("out")

  features <- as.character(features)
  pipeline <- ml_pipeline(sc) %>%
    ft_vector_assembler(features, assembled) %>%
    ft_pca(assembled, out, k = k)

  pipeline_model <- pipeline %>%
    ml_fit(x)

  m <- new_ml_model(
    pipeline_model = pipeline_model,
    formula = paste0("~ ", paste0(features, collapse = " + ")),
    dataset = x,
    class = "ml_model_pca"
  )

  model <- m$model

  pc <- model$pc
  pc_names <- paste0(pc_prefix, seq_len(ncol(pc)))
  rownames(pc) <- features
  colnames(pc) <- pc_names

  explained_variance <- model$explained_variance

  if (!is.null(explained_variance)) {
    names(explained_variance) <- pc_names
  }

  m$k <- k
  m$pc <- pc
  m$explained_variance <- explained_variance

  m
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

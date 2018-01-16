#' Extracts data associated with a Spark ML model
#'
#' @param object a Spark ML model
#' @return A tbl_spark
#' @export
ml_model_data <- function(object) {
  sdf_register(object$data)
}

try_null <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

#' @export
predict.ml_model_classification <- function(object,
                                            newdata = ml_model_data(object),
                                            ...)
{
  ml_predict(object, newdata) %>%
    sdf_read_column("predicted_label")
}

#' @export
predict.ml_model_regression <- function(
  object, newdata = ml_model_data(object), ...
) {
  prediction_col <- ml_param(object$model, "prediction_col")

  ml_predict(object, newdata) %>%
    sdf_read_column(prediction_col)
}

#' @export
fitted.ml_model <- function(object, ...) {

  predictions <- object$.model %>%
    invoke("summary") %>%
    invoke("predictions")

  id <- object$model.parameters$id
  object$data %>%
    invoke("join", predictions, as.list(id)) %>%
    sdf_read_column("prediction")
}

#' @export
residuals.ml_model <- function(object, ...) {
  stop(paste0("'residuals()' not yet supported for ",
              setdiff(class(object), "ml_model"))
  )
}

#' Model Residuals
#'
#' This generic method returns a Spark DataFrame with model
#' residuals added as a column to the model training data.
#'
#' @param object Spark ML model object.
#' @param ... additional arguments
#'
#' @rdname sdf_residuals
#'
#' @export
sdf_residuals <- function(object, ...) {
  UseMethod("sdf_residuals")
}

read_spark_vector <- function(jobj, field) {
  object <- invoke(jobj, field)
  invoke(object, "toArray")
}

read_spark_matrix <- function(jobj, field = NULL) {
  object <- if (rlang::is_null(field)) jobj else invoke(jobj, field)
  nrow <- invoke(object, "numRows")
  ncol <- invoke(object, "numCols")
  data <- invoke(object, "toArray")
  matrix(data, nrow = nrow, ncol = ncol)
}

ml_short_type <- function(x) {
  jobj_class(spark_jobj(x))[1]
}

jobj_set_param <- function(jobj, method, param, default, min_version) {
  ver <- jobj %>%
    spark_connection() %>%
    spark_version()

  if (ver < min_version) {
    if (!identical(param, default))
      stop(paste0("Param '", deparse(substitute(param)),
                  "' is only available for Spark ", min_version, " and later"))
    else
      jobj
  } else {
    jobj %>%
      invoke(method, param)
  }
}

spark_dense_matrix <- function(sc, mat) {
  invoke_new(
    sc, "org.apache.spark.ml.linalg.DenseMatrix", dim(mat)[1L], dim(mat)[2L],
    as.list(mat))
}

spark_dense_vector <- function(sc, vec) {
  invoke_static(sc,  "org.apache.spark.ml.linalg.Vectors", "dense",
                as.list(vec))
}

spark_sql_column <- function(sc, col, alias = NULL) {
  jobj <- invoke_new(sc, "org.apache.spark.sql.Column", col)
  if (!is.null(alias))
    jobj <- invoke(jobj, "alias", alias)
  jobj
}

make_approx_nearest_neighbors <- function(jobj) {
  force(jobj)
  function(dataset, key, num_nearest_neighbors, dist_col = "distCol") {
    dataset <- spark_dataframe(dataset)
    key <- spark_dense_vector(spark_connection(jobj), key)
    num_nearest_neighbors <- ensure_scalar_integer(num_nearest_neighbors)
    dist_col <- ensure_scalar_character(dist_col)
    jobj %>%
      invoke("approxNearestNeighbors",
             dataset, key, num_nearest_neighbors, dist_col) %>%
      sdf_register()
  }
}

make_approx_similarity_join <- function(jobj) {
  function(dataset_a, dataset_b, threshold, dist_col = "distCol") {
    sc <- spark_connection(jobj)
    dataset_a <- spark_dataframe(dataset_a)
    dataset_b <- spark_dataframe(dataset_b)
    threshold <- ensure_scalar_double(threshold)
    dist_col <- ensure_scalar_character(dist_col)
    jobj %>%
      invoke("approxSimilarityJoin",
             dataset_a, dataset_b, threshold, dist_col) %>%
      invoke("select", list(
        spark_sql_column(sc, "datasetA.id", "id_a"),
        spark_sql_column(sc, "datasetB.id", "id_b"),
        spark_sql_column(sc, dist_col)
      )) %>%
      sdf_register()
  }
}

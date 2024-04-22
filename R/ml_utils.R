#' Extracts data associated with a Spark ML model
#'
#' @param object a Spark ML model
#' @return A tbl_spark
#' @export
ml_model_data <- function(object) {
  sdf_register(object$data)
}

possibly_null <- function(.f) purrr::possibly(.f, otherwise = NULL)

#' @export
predict.ml_model_classification <- function(object,
                                            newdata = ml_model_data(object),
                                            ...) {
  ml_predict(object, newdata) %>%
    sdf_read_column("predicted_label")
}

#' @export
predict.ml_model_regression <- function(object, newdata = ml_model_data(object), ...) {
  prediction_col <- ml_param(object$model, "prediction_col")

  ml_predict(object, newdata) %>%
    sdf_read_column(prediction_col)
}

#' @export
fitted.ml_model_prediction <- function(object, ...) {
  prediction_col <- object$model %>%
    ml_param("prediction_col")
  object %>%
    ml_predict() %>%
    dplyr::pull(!!rlang::sym(prediction_col))
}

#' @export
residuals.ml_model <- function(object, ...) {
  stop("'residuals()' not supported for ", class(object)[[1L]])
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

spark_dense_matrix <- function(sc, mat) {
  if (is.null(mat)) {
    return(mat)
  }
  invoke_new(
    sc, "org.apache.spark.ml.linalg.DenseMatrix", dim(mat)[1L], dim(mat)[2L],
    as.list(mat)
  )
}

spark_dense_vector <- function(sc, vec) {
  if (is.null(vec)) {
    return(vec)
  }
  invoke_static(
    sc, "org.apache.spark.ml.linalg.Vectors", "dense",
    as.list(vec)
  )
}

spark_sql_column <- function(sc, col, alias = NULL) {
  jobj <- invoke_new(sc, "org.apache.spark.sql.Column", col)
  if (!is.null(alias)) {
    jobj <- invoke(jobj, "alias", alias)
  }
  jobj
}

make_stats_arranger <- function(fit_intercept) {
  if (fit_intercept) {
    function(x) {
      force(x)
      c(tail(x, 1), head(x, length(x) - 1))
    }
  } else {
    identity
  }
}

# ----------------------------- ML helpers -------------------------------------

ml_process_model <- function(x, uid, r_class, invoke_steps, ml_function,
                             formula = NULL, response = NULL, features = NULL) {
  sc <- spark_connection(x)

  # Mapping R class to Spark model using /inst/sparkml/class_mapping.json
  class_mapping <- as.list(genv_get_ml_class_mapping())
  spark_class <- names(class_mapping[class_mapping == r_class])

  args <- list(sc, spark_class)
  if (!is.null(uid)) {
    uid <- cast_string(uid)
    args <- append(args, list(uid))
  }

  jobj <- do.call(invoke_new, args)

  pe <- params_validate_estimator_and_set(jobj, invoke_steps)

  l_steps <- purrr::imap(pe, ~ list(.y, .x))

  for(i in seq_along(l_steps)) {
    if(!is.null(l_steps[[i]][[2]])) {
      jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
    }
  }

  new_estimator <- new_ml_estimator(jobj, class = r_class)

  post_ml_obj(
    x = x,
    nm = new_estimator,
    ml_function = ml_function,
    formula = formula,
    response = response,
    features = features,
    features_col = invoke_steps$features_col,
    label_col = invoke_steps$label_col
  )

}

param_min_version <- function(x, value, min_version = NULL, default = NULL) {
  res <- value
  if (!is.null(value)) {
    if (!is.null(min_version)) {
      sc <- spark_connection(x)
      ver <- spark_version(sc)
      if (ver < min_version) {
        if(value != default) {
          stop(paste0(
            "Parameter `", deparse(substitute(value)),
            "` is only available for Spark ", min_version, " and later.",
            "To avoid passing this variable, change the argument value to NULL."
          ))
        } else {
          res <- NULL
        }
      }
    }
  }
  res
}

# --------------------- Post conversion functions ------------------------------

post_ml_obj <- function(x, nm, ml_function, formula, response,
                        features, features_col, label_col) {
  UseMethod("post_ml_obj")
}

#' @export
post_ml_obj.spark_connection <- function(x, nm, ml_function, formula, response,
                                         features, features_col, label_col) {
  nm
}

#' @export
post_ml_obj.ml_pipeline <- function(x, nm, ml_function, formula, response,
                                    features, features_col, label_col) {
  ml_add_stage(x, nm)
}

#' @export
post_ml_obj.tbl_spark <- function(x, nm, ml_function, formula, response,
                                  features, features_col, label_col) {
  formula <- ml_standardize_formula(formula, response, features)

  if (is.null(formula)) {
    ml_fit(nm, x)
  } else {
    ml_construct_model_supervised(
      ml_function,
      predictor = nm,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col
    )
  }
}


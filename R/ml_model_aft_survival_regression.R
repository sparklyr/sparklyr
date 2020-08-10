new_ml_model_aft_survival_regression <- function(pipeline_model, formula, dataset, label_col,
                                                 features_col) {
  m <- new_ml_model_regression(
    pipeline_model,
    formula = formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_aft_survival_regression"
  )

  model <- m$model
  jobj <- spark_jobj(model)

  coefficients <- model$coefficients
  names(coefficients) <- m$feature_names

  m$coefficients <- if (ml_param(model, "fit_intercept")) {
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", m$feature_names)
    )
  }

  m
}

#' @export
print.ml_model_aft_survival_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

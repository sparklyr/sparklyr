new_ml_model_naive_bayes <- function(pipeline_model, formula, dataset, label_col,
                                     features_col, predicted_label_col) {
  m <- new_ml_model_classification(
    pipeline_model, formula,
    dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_naive_bayes"
  )

  model <- m$model

  pi <- model$pi
  names(pi) <- m$index_labels
  m$pi <- pi

  theta <- model$theta
  rownames(theta) <- m$index_labels
  colnames(theta) <- m$feature_names
  m$theta <- theta

  m
}

# Generic implementations

#' @export
print.ml_model_naive_bayes <- function(x, ...) {
  printf("A-priori probabilities:\n")
  print(exp(sort_named_list(x$pi)))
  print_newline()

  printf("Conditional probabilities:\n")
  print(exp(x$theta[order(row.names(x$theta)), ]))
  print_newline()

  x
}

#' @export
summary.ml_model_naive_bayes <- function(object, ...) {
  print(object, ...)
}

new_ml_model_naive_bayes <- function(pipeline, pipeline_model, model,
                                     dataset, formula, feature_names,
                                     index_labels, call) {

  pi <- model$pi
  names(pi) <- index_labels

  theta <- model$theta
  rownames(theta) <- index_labels
  colnames(theta) <- feature_names


  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_naive_bayes",
    !!!list(pi = pi,
            theta = theta,
            .features = feature_names,
            .index_labels = index_labels)
  )
}

# Generic implementations

#' @export
print.ml_model_naive_bayes <- function(x, ...) {
  printf("A-priority probabilities:\n")
  print(exp(x$pi))
  print_newline()

  printf("Conditional probabilities:\n")
  print(exp(x$theta))
  print_newline()

  x
}

#' @export
summary.ml_model_naive_bayes <- function(object, ...) {
  print(object, ...)
}

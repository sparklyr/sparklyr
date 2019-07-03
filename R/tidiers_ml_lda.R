#' Tidying methods for Spark ML LDA models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_lda_tidiers
NULL

#' @rdname ml_lda_tidiers
#' @importFrom rlang !!
#' @export
tidy.ml_model_lda <- function(x,
                              ...){

  term <- ml_vocabulary(x)
  topics_matrix <- x$model$topics_matrix() %>%
    dplyr::as_tibble()

  k <- x$model$param_map$k
  names(topics_matrix) <- 0:(k - 1)

  dplyr::bind_cols(term = term, topics_matrix) %>%
    tidyr::gather(!!"topic", beta, -term, convert = TRUE) %>%
    dplyr::select(!!"topic", term, beta)
}

#' @rdname ml_lda_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#' @importFrom rlang syms
#' @export
augment.ml_model_lda <- function(x, newdata = NULL,
                                 ...){

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  vars <- c(dplyr::tbl_vars(newdata), "topicDistribution")

  ml_predict(x, newdata ) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.topic = !!"topicDistribution")
}

#' @rdname ml_lda_tidiers
#' @export
glance.ml_model_lda <- function(x,
                                ...){

  k = x$model$param_map$k
  vocab_size <- x$model$vocab_size
  learning_decay <- x$model$param_map$learning_decay
  optimizer <- x$model$param_map$optimizer

  dplyr::tibble(k = k,
                vocab_size = vocab_size,
                learning_decay = learning_decay,
                optimizer = optimizer)
}

#' Tidying methods for Spark ML LDA models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_lda_tidiers
NULL

#' @rdname ml_lda_tidiers
#' @export
tidy.ml_lda_model <- function(x,
                              ...){

 doc_concentration <- sparklyr::invoke(x$model$estimated_doc_concentration(),
                                       "toArray")

 describe_topics <- x$model$describe_topics() %>%
   dplyr::collect()

 cbind(describe_topics,
       doc_concentrarion = doc_concentration) %>%
    dplyr::as_tibble()
}

#' @rdname ml_lda_tidiers
#'
#' @export
augment.ml_lda_model <- function(x,
                                 ...){

  vocabulary <- ml_vocabulary(x)
  topics_matrix <- x$model$topics_matrix() %>%
    dplyr::as_data_frame()

  k <- x$model$param_map$k
  names(topics_matrix) <- purrr::map_chr(0:(k - 1), function(e){
    paste0("topic_", e) # the topics in spark start with 0
  })

  dplyr::bind_cols(vocabulary = vocabulary,
                   topics_matrix)
}

#' @rdname ml_lda_tidiers
#' @export
glance.ml_lda_model <- function(x,
                                ...) {

  k = x$model$param_map$k
  vocab_size <- x$model$vocab_size
  learning_decay <- x$model$param_map$learning_decay
  optimizer <- x$model$param_map$optimizer

  dplyr::tibble(k = k,
                vocab_size = vocab_size,
                learning_decay = learning_decay,
                optimizer = optimizer)
}

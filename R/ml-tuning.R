#' @export
ml_param_grid <- function(param_list) {
  names(param_list) %>% # stages
    lapply(function(stage) {
      stage_params <- param_list[[stage]]
      params <- names(stage_params)
      rlang::set_names(lapply(params, function(param) { # params
        param_values <- stage_params[[param]]
        lapply(param_values, function(value) { # values
          list(stage = stage, param = param, value = value)
        })
      }), paste(stage, params, sep = "-"))
    }) %>%
    rlang::flatten() %>%
    expand.grid(stringsAsFactors = FALSE) %>%
    apply(1, list) %>%
    rlang::flatten()
}

#' @export
ml_cross_validator <- function(x, estimator, param_maps
                               # evaluator,
                               # num_folds, seed,
                               # uid = random_string("cross_validator_")
) {
  UseMethod("ml_cross_validator")
}

#' @export
ml_cross_validator.spark_connection <- function(x, estimator, param_maps
                                                # evaluator,
                                                # num_folds,
                                                # seed,
                                                # uid = random_string("cross_validator_")
) {
  sc <- x
  estimator <- estimator$.jobj
  uid_stages <- invoke_static(sc,
                              "sparklyr.MLUtils",
                              "uidStagesMapping",
                              estimator)

  param_maps <- ml_param_grid(param_maps) %>%
    lapply(function(param_map) Reduce(
      # function to put ParamMap to ParamMap
      function(x, param_pair) invoke(x, "put", param_pair$param, param_pair$value),
      # returns pairs (Param, value) to be folded
      lapply(param_map, function(stage_param) uid_stages %>%
               `[[`(grep(paste0("^", stage_param$stage), names(uid_stages))) %>%
               invoke(param_mapping_r_to_s[[stage_param$param]] %||% stage_param$param) %>%
               list(stage_param$value) %>%
               rlang::set_names(c("param", "value"))
      ),
      # empty ParamMap to initialize Reduce
      invoke_new(sc, "org.apache.spark.ml.param.ParamMap")
    ))

  jobj <- invoke_new(sc, "org.apache.spark.ml.tuning.CrossValidator") %>%
    (function(cv) invoke_static(sc, "sparklyr.MLUtils", "setParamMaps",
                                cv, param_maps)) %>%
    invoke("setEstimator", estimator)

  param_maps <- jobj %>%
    invoke("getEstimatorParamMaps") %>%
    lapply(function(x) invoke_static(sc, "sparklyr.MLUtils",
                                     "paramMapToList", x)) %>%
    lapply(function(param_map) {
      # param_names <- param_map %>%
      #   sapply(function(x) paste(x[[1]], ml_map_param_name(x[[2]]),
      #                            sep = "-"))
      # rlang::set_names(param_map, param_names) %>%
      param_map %>%
        lapply(rlang::set_names, c("stage", "param", "value")) %>%
        lapply(function(x) rlang::modify(x, param = ml_map_param_name(x[["param"]]))) %>%
        (function(x) rlang::set_names(x, sapply(x, function(stage_param) paste(
          stage_param[[1]], stage_param[[2]], sep = "-"))))
    })

  structure(
    list(
      type = jobj_info(jobj)$class,
      param_maps = param_maps,
      .jobj = jobj
    ),
    class = "ml_cross_validator"
  )
}

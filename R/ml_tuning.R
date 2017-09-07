#' @export
ml_build_params <- function(param_list) {
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
ml_expand_params <- function(param_list) {
  param_list %>%
    lapply(function(stage) {
      params <- names(stage)
      lapply(params, function(param) {
        param_values <- stage[[param]]
        param_values %>%
          lapply(function(value) {
            rlang::set_names(list(value = value), param)
          })
      }) %>%
        expand.grid(stringsAsFactors = FALSE) %>%
        apply(1, list) %>%
        rlang::flatten() %>%
        lapply(function(x) x %>%
                 unname() %>%
                 rlang::flatten())
    })
}

#' @export
ml_params_by_stage <- function(param_list) {
  param_list %>%
    lapply(
      function(param_map) param_map %>%
        lapply(function(stage_param) {
          list(
            param_pair = rlang::set_names(list(stage_param$value), stage_param$param)
          ) %>%
            rlang::set_names(stage_param$stage)
        }) %>%
        unname() %>%
        rlang::flatten() %>%
        (function(x) split(x, names(x))) %>%
        lapply(function(x) x %>%
                 unname() %>%
                 rlang::flatten())
    ) %>%
    rlang::flatten() %>%
    (function(x) split(x, names(x))) %>%
    lapply(unname) %>%
    lapply(unique)
}

#' @export
ml_cross_validator <- function(x, estimator, estimator_param_maps
                               # evaluator,
                               # num_folds, seed,
                               # uid = random_string("cross_validator_")
) {
  UseMethod("ml_cross_validator")
}

#' @export
ml_cross_validator.spark_connection <- function(x, estimator, estimator_param_maps
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

  current_param_list <- uid_stages %>%
    lapply(invoke, "extractParamMap") %>%
    lapply(function(x) invoke_static(sc,
                                     "sparklyr.MLUtils",
                                     "paramMapToList",
                                     x))

  param_maps <- estimator_param_maps %>%
    ml_expand_params() %>%
    (function(stages_params) {
      stage_names <- names(stages_params)
      stage_uids <- sapply(paste0("^", stage_names), grep, names(uid_stages), value = TRUE) %>%
        rlang::set_names(stage_names)
      stage_names %>%
        lapply(function(stage_name) {
          stage_class <- uid_stages %>%
            `[[`(stage_uids[stage_name]) %>%
            jobj_info() %>%
            `[[`("class")
          lapply(stages_params[[stage_name]], function(params) {
            do.call(ml_get_stage_validator(stage_class),
                    list(.args = params,
                         .current_args = current_param_list %>%
                           `[[`(stage_uids[stage_name]) %>%
                           ml_map_param_list_names()
                    )
            )
          })
        }) %>%
        rlang::set_names(stage_uids)
    }) %>%
    expand.grid(stringsAsFactors = FALSE) %>%
    apply(1, list) %>%
    rlang::flatten() %>%
    lapply(function(param_map) {
      stage_uids <- names(param_map)
      param_jobj_value_list <- stage_uids %>%
        lapply(function(stage_uid) {
          # params <- param_map
          # param_names <- names(param_map[[stage_uid]])
          # values <- param_map[[stage_uid]] %>%
          #   rlang::set_names(param_names)
          # param_names %>%
          params <- param_map[[stage_uid]]
          names(params) %>%
            lapply(function(param_name) {
              list(param_jobj = uid_stages[[stage_uid]] %>%
                     invoke(sparklyr:::ml_map_param_names(param_name, "rs")),
                   value = params[[param_name]])
            })
        }) %>%
        rlang::flatten()

      Reduce(function(x, pair) invoke(x, "put", pair$param_jobj, pair$value),
             param_jobj_value_list,
             invoke_new(sc, "org.apache.spark.ml.param.ParamMap"))
    })

  jobj <- invoke_new(sc, "org.apache.spark.ml.tuning.CrossValidator") %>%
    (function(cv) invoke_static(sc, "sparklyr.MLUtils", "setParamMaps",
                                cv, param_maps)) %>%
    invoke("setEstimator", estimator)

  param_maps <- jobj %>%
    invoke("getEstimatorParamMaps") %>%
    lapply(function(x) invoke_static(sc, "sparklyr.MLUtils",
                                     "paramMapToTriples", x)) %>%
    lapply(function(param_map) {
      param_map %>%
        lapply(rlang::set_names, c("stage", "param", "value")) %>%
        lapply(function(x) rlang::modify(x, param = ml_map_param_names(x[["param"]]))) %>%
        (function(x) rlang::set_names(x, sapply(x, function(stage_param) paste(
          stage_param[[1]], stage_param[[2]], sep = "-"))))
    })

  structure(
    list(
      type = jobj_info(jobj)$class,
      estimator_param_maps = param_maps,
      .jobj = jobj
    ),
    class = "ml_cross_validator"
  )
}

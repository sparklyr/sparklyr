ml_expand_params <- function(param_grid) {
  param_grid %>%
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

ml_validate_params <- function(stages_params, uid_stages, current_param_list) {
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
        args_to_validate <- ml_args_to_validate(
          args = params,
          current_args = current_param_list %>%
            `[[`(stage_uids[stage_name]) %>%
            ml_map_param_list_names(),
          default_args = Filter(
            Negate(rlang::is_symbol),
            stage_class %>%
              ml_get_stage_constructor() %>%
              rlang::fn_fmls() %>%
              rlang::modify(uid = NULL)
          ))
        rlang::invoke(ml_get_stage_validator(stage_class),
                      args = args_to_validate)
      })
    }) %>%
    rlang::set_names(stage_uids)
}

ml_build_param_maps <- function(param_list) {
  param_list %>%
    expand.grid(stringsAsFactors = FALSE) %>%
    apply(1, list) %>%
    rlang::flatten()
}

ml_spark_param_map <- function(param_map, sc, uid_stages) {
  stage_uids <- names(param_map)
  param_jobj_value_list <- stage_uids %>%
    lapply(function(stage_uid) {
      params <- param_map[[stage_uid]]
      Filter(function(x) !rlang::is_null(x$value),
             names(params) %>%
               lapply(function(param_name) {
                 list(param_jobj = uid_stages[[stage_uid]] %>%
                        invoke(sparklyr:::ml_map_param_names(param_name, "rs")),
                      value = params[[param_name]]
                 )
               }))
    }) %>%
    rlang::flatten()

  Reduce(function(x, pair) invoke(x, "put", pair$param_jobj, pair$value),
         param_jobj_value_list,
         invoke_new(sc, "org.apache.spark.ml.param.ParamMap"))
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
    ml_validate_params(uid_stages, current_param_list) %>%
    ml_build_param_maps() %>%
    lapply(ml_spark_param_map, sc, uid_stages)

  jobj <- invoke_new(sc, "org.apache.spark.ml.tuning.CrossValidator") %>%
    (function(cv) invoke_static(sc, "sparklyr.MLUtils", "setParamMaps",
                                cv, param_maps)) %>%
    invoke("setEstimator", estimator)

  new_ml_cross_validator(jobj)
}

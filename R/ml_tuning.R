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
        # compute param-value combinations within each stage
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
  # match stage names
  stage_uids <- sapply(paste0("^", stage_names), grep, names(uid_stages), value = TRUE) %>%
    rlang::set_names(stage_names)
  stage_names %>%
    lapply(function(stage_name) {
      stage_jobj <- uid_stages %>%
        `[[`(stage_uids[stage_name])
      # %>%
      #   jobj_class(simple_name = FALSE) %>%
      #   dplyr::first()
      lapply(stages_params[[stage_name]], function(params) {
        args_to_validate <- ml_args_to_validate(
          args = params,
          # current param list parsed from the pipeline jobj
          current_args = current_param_list %>%
            `[[`(stage_uids[stage_name]) %>%
            ml_map_param_list_names(),
          # default args from the stage constructor, excluding args with no default
          #   and `uid`
          default_args = Filter(
            Negate(rlang::is_symbol),
            stage_jobj %>%
              ml_get_stage_constructor() %>%
              rlang::fn_fmls() %>%
              rlang::modify(uid = NULL)
          ))
        # calls the appropriate validator and returns a list
        rlang::invoke(ml_get_stage_validator(stage_jobj),
                      args = args_to_validate,
                      nms = names(params))
      })
    }) %>%
    rlang::set_names(stage_uids)
}

ml_build_param_maps <- function(param_list) {
  # computes combinations at the stages level
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
             # only create param_map with non-null values
             names(params) %>%
               lapply(function(param_name) {
                 # get the Param object by calling `[stage].[param]` in Scala
                 list(param_jobj = uid_stages[[stage_uid]] %>%
                        invoke(ml_map_param_names(param_name, "rs")),
                      value = params[[param_name]]
                 )
               }))
    }) %>%
    rlang::flatten()

  # put the param pairs into a ParamMap
  Reduce(function(x, pair) invoke(x, "put", pair$param_jobj, pair$value),
         param_jobj_value_list,
         invoke_new(sc, "org.apache.spark.ml.param.ParamMap"))
}


param_maps_to_df <- function(param_maps) {
  param_maps %>%
    lapply(function(param_map) {
      param_map %>%
        lapply(data.frame, stringsAsFactors = FALSE) %>%
        (function(x) lapply(seq_along(x), function(n) {
          fn <- function(x) paste(x, n, sep = "_S")
          dplyr::rename_all(x[[n]], fn)
        })) %>% dplyr::bind_cols()
    }) %>%
    dplyr::bind_rows()
}

ml_get_estimator_param_maps <- function(jobj) {
  sc <- spark_connection(jobj)
  jobj %>%
    invoke("getEstimatorParamMaps") %>%
    lapply(function(x)
      invoke_static(sc,
                    "sparklyr.MLUtils",
                    "paramMapToNestedList",
                    x)) %>%
    lapply(function(x)
      lapply(x, ml_map_param_list_names))
}

ml_new_validator <- function(
  sc, class, estimator, evaluator, estimator_param_maps, seed) {
  seed <- ensure_scalar_integer(seed, allow.null = TRUE)

  if (!inherits(evaluator, "ml_evaluator"))
    stop("evaluator must be a 'ml_evaluator'")
  if (!is_ml_estimator(estimator))
    stop("estimator must be a 'ml_estimator'")

  uid_stages <- if (inherits(estimator, "ml_pipeline"))
    invoke_static(sc,
                  "sparklyr.MLUtils",
                  "uidStagesMapping",
                  spark_jobj(estimator)) else
    rlang::set_names(list(spark_jobj(estimator)), ml_uid(estimator))

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

  jobj <- invoke_new(sc, class) %>%
    (function(cv) invoke_static(sc, "sparklyr.MLUtils", "setParamMaps",
                                cv, param_maps)) %>%
    invoke("setEstimator", spark_jobj(estimator)) %>%
    invoke("setEvaluator", spark_jobj(evaluator))

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  jobj
}

new_ml_tuning <- function(jobj, ..., subclass = NULL) {
  new_ml_estimator(jobj,
                   estimator = invoke(jobj, "getEstimator") %>%
                     ml_constructor_dispatch(),
                   evaluator = invoke(jobj, "getEvaluator") %>%
                     ml_constructor_dispatch(),
                   estimator_param_maps = ml_get_estimator_param_maps(jobj),
                   ...,
                   subclass = c(subclass, "ml_tuning"))
}

new_ml_tuning_model <- function(jobj, ..., subclass = NULL) {
  # TODO move metrics here
  new_ml_transformer(
    jobj,
    best_model = ml_constructor_dispatch(invoke(jobj, "bestModel")),
    ...,
    subclass = c(subclass, "ml_tuning_model"))
}

# Param Grid Transformation
#
# This is an overview of the helper functions in `ml_cross_validator()` that enable
#   the interpretation of hyperparameter grids. Specifically, we look at the following
#   transformations:
#
# . %>%
#   ml_expand_params() %>%
#   ml_validate_params(uid_stages, current_param_list) %>%
#   ml_build_param_maps() %>%
#   lapply(ml_spark_param_map, sc, uid_stages)
#
# Users pass a nested list to `ml_cross_validator()`, e.g.
# param_grid <- list(
#   hash = list(
#     num_features = list(2^5, 2^10)
#   ),
#   logistic = list(
#     max_iter = list(5, 10),
#     reg_param = list(0.01, 0.001)
#   )
# )
#
# list 2
# .  hash = list 1
# . .  num_features = list 2
# . . .  [[1]] = double 1= 32
# . . .  [[2]] = double 1= 1024
# .  logistic = list 2
# . .  max_iter = list 2
# . . .  [[1]] = double 1= 5
# . . .  [[2]] = double 1= 10
# . .  reg_param = list 2
# . . .  [[1]] = double 1= 0.01
# . . .  [[2]] = double 1= 0.001
#
# The `ml_expand_params()` function then takes this list and computes all
#   combinations of param-value pairs within each stage:
#
# list 2
# .  hash = list 2
# . .  [[1]] = list 1
# . . .  num_features = double 1= 32
# . .  [[2]] = list 1
# . . .  num_features = double 1= 1024
# .  logistic = list 4
# . .  [[1]] = list 2
# . . .  max_iter = double 1= 5
# . . .  reg_param = double 1= 0.01
# . .  [[2]] = list 2
# . . .  max_iter = double 1= 10
# . . .  reg_param = double 1= 0.01
# . .  [[3]] = list 2
# . . .  max_iter = double 1= 5
# . . .  reg_param = double 1= 0.001
# . .  [[4]] = list 2
# . . .  max_iter = double 1= 10
# . . .  reg_param = double 1= 0.001
#
# `ml_validate_params()` takes this output and performs input validation within each stage.
#   It matches abbreviated stage names to complete stage names and augments the grid
#   with arguments currently specified in the pipeline and default arguments from the stage
#   constructors.
#
# list 2
# .  hashing_tf_1 = list 2
# . .  [[1]] = list 4
# . . .  num_features = integer 1= 32
# . . .  binary = logical 1= FALSE
# . . .  output_col = character 1= features
# . . .  input_col = character 1= words
# . .  [[2]] = list 4
# . . .  num_features = integer 1= 1024
# . . .  binary = logical 1= FALSE
# . . .  output_col = character 1= features
# . . .  input_col = character 1= words
# .  logistic_1 = list 4
# . .  [[1]] = list 16
# . . .  max_iter = integer 1= 5
# . . .  reg_param = double 1= 0.01
# . . .  elastic_net_param = double 1= 0
# . . .  aggregation_depth = integer 1= 2
# . . .  probability_col = character 1= probability
# . . .  features_col = character 1= features
# . . .  fit_intercept = logical 1= TRUE
# . . .  label_col = character 1= label
# . . .  raw_prediction_col = character 1= rawPrediction
# . . .  family = character 1= auto
# . . .  standardization = logical 1= TRUE
# . . .  threshold = double 1= 0.5
# . . .  ...   and 4 more
# (etc.)
#
# Then, `ml_build_param_maps()` builds param maps on the R side, by computing combinations
#   of different specifications of *stages*. The output is a list with length the product
#   of number of values input for each param.
#
# after build param maps
# list 8
# .  [[1]] = list 2
# . .  hashing_tf_1 = list 4
# . . .  num_features = integer 1= 32
# . . .  binary = logical 1= FALSE
# . . .  output_col = character 1= features
# . . .  input_col = character 1= words
# . .  logistic_1 = list 16
# . . .  max_iter = integer 1= 5
# . . .  reg_param = double 1= 0.01
# . . .  elastic_net_param = double 1= 0
# . . .  aggregation_depth = integer 1= 2
# . . .  probability_col = character 1= probability
# . . .  features_col = character 1= features
# . . .  fit_intercept = logical 1= TRUE
# . . .  label_col = character 1= label
# . . .  raw_prediction_col = character 1= rawPrediction
# . . .  family = character 1= auto
# . . .  standardization = logical 1= TRUE
# . . .  threshold = double 1= 0.5
# . . .  ...   and 4 more
# (etc.)
#
# Finally, the `lapply(ml_spark_param_map)` call converts the R param maps to
#   Spark param maps:
#
# List of 8
# $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x11c800270>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x11994a158>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x11931dd80>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x119ceb1f8>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x11a83c158>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x11a4ffd18>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x118e59468>
#   $ :Classes 'spark_jobj', 'shell_jobj' <environment: 0x1184bdf90>

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
      stage_class <- uid_stages %>%
        `[[`(stage_uids[stage_name]) %>%
        jobj_info() %>%
        `[[`("class")
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
            stage_class %>%
              ml_get_stage_constructor() %>%
              rlang::fn_fmls() %>%
              rlang::modify(uid = NULL)
          ))
        # calls the appropriate validator and returns a list
        rlang::invoke(ml_get_stage_validator(stage_class),
                      args = args_to_validate)
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
                        invoke(sparklyr:::ml_map_param_names(param_name, "rs")),
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

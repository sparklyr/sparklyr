extract_estimate_statistics <- function(object, stat_names, new_names) {
  stat_names %>%
    lapply(function(x) object[[x]]) %>%
    as.data.frame() %>%
    broom::fix_data_frame(newnames = new_names)
}

extract_model_metrics <- function(object, metric_names, new_names) {
  sapply(metric_names, function(x) object[[x]]) %>%
    t() %>%
    broom::fix_data_frame(newnames = new_names)
}

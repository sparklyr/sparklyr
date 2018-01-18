
df_from_sql <- function(sc, sql) {
  sdf <- invoke(hive_context(sc), "sql", as.character(sql))
  df_from_sdf(sc, sdf)
}

df_from_sdf <- function(sc, sdf, take = -1) {
  sdf_collect(sdf)
}

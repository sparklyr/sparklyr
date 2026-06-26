translate_spark_column_types <- function(sdf) {
  type_map <- list(
    BooleanType = "logical",
    ByteType = "integer",
    ShortType = "integer",
    IntegerType = "integer",
    FloatType = "numeric",
    DoubleType = "numeric",
    LongType = "numeric",
    StringType = "character",
    BinaryType = "raw",
    TimestampType = "POSIXct",
    DateType = "Date",
    CalendarIntervalType = "character",
    NullType = "NULL"
  )

  sdf %>%
    sdf_schema() %>%
    lapply(
      function(e) {
        if (e$type %in% names(type_map)) {
          type_map[[e$type]]
        } else if (grepl("^(Array|Struct|Map)Type\\(.*\\)$", e$type)) {
          "list"
        } else if (grepl("^DecimalType\\(.*\\)$", e$type)) {
          "numeric"
        } else {
          "unknown"
        }
      }
    )
}


cast_string <- function(x) {
  vctrs::vec_check_size(
    x,
    1,
    arg = rlang::caller_arg(x),
    call = rlang::caller_env()
  )
  vctrs::vec_cast(x, character())
}


cast_scalar_logical <- function(x) {
  vctrs::vec_check_size(
    x,
    1,
    arg = rlang::caller_arg(x),
    call = rlang::caller_env()
  )
  vctrs::vec_cast(x, logical())
}


cast_scalar_double <- function(x) {
  vctrs::vec_check_size(
    x,
    1,
    arg = rlang::caller_arg(x),
    call = rlang::caller_env()
  )
  vctrs::vec_cast(x, numeric())
}


cast_scalar_integer <- function(x) {
  vctrs::vec_check_size(
    x,
    1,
    arg = rlang::caller_arg(x),
    call = rlang::caller_env()
  )
  vctrs::vec_cast(x, integer())
}


cast_nullable_string <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  cast_string(x)
}


cast_nullable_scalar_double <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  cast_scalar_double(x)
}


cast_nullable_scalar_integer <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  cast_scalar_integer(x)
}


cast_double <- function(x) {
  vctrs::vec_cast(x, numeric())
}


cast_integer <- function(x) {
  vctrs::vec_cast(x, integer())
}


cast_list <- function(x, ptype, allow_null = FALSE) {
  if (is.null(x)) {
    if (allow_null) {
      return(NULL)
    } else {
      rlang::abort("{.arg x} must not be `NULL`.")
    }
  }

  if (is.list(x)) {
    x <- vctrs::list_unchop(x)
  }
  x <- vctrs::vec_cast(x, to = ptype)
  vctrs::vec_chop(x)
}


cast_string_list <- function(x, allow_null = FALSE) {
  cast_list(x, character(), allow_null = allow_null)
}


cast_integer_list <- function(x, allow_null = FALSE) {
  cast_list(x, integer(), allow_null = allow_null)
}


cast_double_list <- function(x, allow_null = FALSE) {
  cast_list(x, numeric(), allow_null = allow_null)
}


cast_choice <- function(
  x,
  choices,
  error_arg = rlang::caller_arg(x),
  error_call = rlang::caller_env()
) {
  rlang::arg_match(x, choices, error_arg = error_arg, error_call = error_call)
}

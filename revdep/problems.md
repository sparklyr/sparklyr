# sparklyr.flint

<details>

* Version: 0.2.2
* GitHub: https://github.com/r-spark/sparklyr.flint
* Source code: https://github.com/cran/sparklyr.flint
* Date/Publication: 2022-01-11 08:50:13 UTC
* Number of recursive dependencies: 56

Run `revdepcheck::revdep_details(, "sparklyr.flint")` for more info

</details>

## Newly broken

*   checking examples ... ERROR
    ```
    Running examples in ‘sparklyr.flint-Ex.R’ failed
    The error most likely occurred in:
    
    > ### Name: asof_future_left_join
    > ### Title: Temporal future left join
    > ### Aliases: asof_future_left_join
    > 
    > ### ** Examples
    > 
    > 
    ...
      5.     └─sparklyr.flint:::new_ts_rdd_builder(...)
      6.       ├─sparklyr::invoke_new(...)
      7.       └─sparklyr:::invoke_new.spark_shell_connection(...)
      8.         ├─sparklyr::invoke_method(sc, TRUE, class, "<init>", ...)
      9.         └─sparklyr:::invoke_method.spark_shell_connection(...)
     10.           └─sparklyr:::core_invoke_method(...)
     11.             └─sparklyr:::core_invoke_method_impl(...)
     12.               └─sparklyr:::spark_error(msg)
     13.                 └─rlang::abort(message = msg, use_cli_format = TRUE, call = NULL)
    Execution halted
    ```


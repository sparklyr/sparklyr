# sparklyr.flint

<details>

* Version: 0.2.2
* GitHub: https://github.com/r-spark/sparklyr.flint
* Source code: https://github.com/cran/sparklyr.flint
* Date/Publication: 2022-01-11 08:50:13 UTC
* Number of recursive dependencies: 57

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
      5.     │ └─base::structure(list(.jobj = jobj), class = "ts_rdd")
      6.     ├─sparklyr::invoke(builder, "fromDF", spark_dataframe(sdf))
      7.     └─sparklyr:::invoke.shell_jobj(builder, "fromDF", spark_dataframe(sdf))
      8.       ├─sparklyr::invoke_method(...)
      9.       └─sparklyr:::invoke_method.spark_shell_connection(...)
     10.         └─sparklyr:::core_invoke_method(...)
     11.           └─sparklyr:::core_invoke_method_impl(...)
     12.             └─sparklyr:::spark_error(msg)
     13.               └─rlang::abort(message = msg, use_cli_format = TRUE, call = NULL)
    Execution halted
    ```


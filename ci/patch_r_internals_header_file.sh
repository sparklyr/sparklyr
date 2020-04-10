#!/bin/bash

cd "$R_INCLUDE_DIR"
sudo patch -l << '_PATCH_R_INTERNALS_H'
*** Rinternals.h   2020-03-19 17:59:06.360000000 +0000
--- Rinternals.h   2020-03-19 17:13:53.464000000 +0000
***************
*** 1210,1215 ****
--- 1210,1216 ----
  #define list3                 Rf_list3
  #define list4                 Rf_list4
  #define list5                 Rf_list5
+ #define list6                 Rf_list6
  #define listAppend            Rf_listAppend
  #define match                 Rf_match
  #define matchE                        Rf_matchE
***************
*** 1374,1377 ****
--- 1375,1380 ----
  }
  #endif

+ #define Rf_list6(x0, x1, x2, x3, x4, x5) ({ PROTECT(x0); x0 = Rf_cons(x0, Rf_list5(x1, x2, x3, x4, x5)); UNPROTECT(1); x0; })
+
  #endif /* R_INTERNALS_H_ */

_PATCH_R_INTERNALS_H

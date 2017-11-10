# Instructions

## New article

1. Add article in the proper repo sub-folder, such as `vignettes`
2. List the new article in the `_blogdown.yml` file
3. Source the `build_site.R` script
4. Run `rebuild_site()` 

## Update an article

1. Make changes in the proper repo sub-folder, such as `vignettes`
2. Delete the existing article in the **blogdown/content** folder or sub-folder inside it
3. Source the `build_site.R` script
4. Run `rebuild_site()` 

## Use an `md` file as the source

1. Change the working directory to the `vignettes` folder
2. Change the article output to `md_output` or `github_document`
3. Knit the `.Rmd` file 
4. Set the working directory back to the repo
5. List the new `.md` file in the `_blogdown.yml` file
6. List the images folder in the `_blogodwn.yml` file, see the path pattern for other `.md` files in the `_blogdown.yml` file
7. Source the `build_site.R` script
8. Run `rebuild_site()` 

## Updates to Reference

1. List any new function in the `reference` section of the `_blogdown.yml` file
3. Source the `build_site.R` script
4. Run `process_reference(overwrite = TRUE))` 


## Full refresh

1. Source the `build_site.R` script
2. Run `rebuild_site(overwrite = TRUE)`

## Preview the site

1. Change the working directory to the **blogdown** sub-directory
2. Run `blogdown::serve_site()`
3. Set the working directory back to the package after done







#' Runs Stock Synthesis in Parallel
#'
#' Runs instances of Stock Synthesis in Parallel via `r4ss::run` for each
#' directory in the input directory list.
#'
#' @import parallel
#' @import checkmate
#' @importFrom r4ss run
#' @importFrom future plan multisession sequential
#' @importFrom future.apply future_lapply
#' @importFrom parallelly availableCores
#' @import progressr
#'
#' @param ss_dirlist Filelist
#' @param ss3_exe ss3_exe
#'
#' @examples
#' \dontrun{
#' run_parallel()
#' }
#'
#'
run_parallel <- function(ss_dirlist, ss3_exe = "ss3.exe") {

  #Validate file list is a list type
  checkmate::assert_list(ss_dirlist, any.missing = FALSE)

  #Validate "Directories" in ss_dirlist exists
  checkmate::assert_directory_exists(unlist(ss_dirlist))

  progressr::with_progress({

    steps <- progressr::progressor(steps = length(ss_dirlist))

    #parallel Loop
    results_list <- future.apply::future_lapply(ss_dirlist, function(x) {

      output <- tryCatch({
        #out <- r4ss::run(dir = x[[1]], exe = ss3_exe, skipfinished = FALSE)
        out <- r4ss::run(dir = x, exe = ss3_exe, skipfinished = FALSE)

        #steps(sprintf("Finished %s", basename(x[[1]] )))
        cli::cli_alert_success("Finished Boot %s")

      }, error = function(e) {
        paste0("Failed at ", x, "\n, Reason: ", e$message)
      })

      steps()

      return(output)

    }, future.seed = TRUE, future.packages = c("r4ss"))

    return(results_list)

  })

}


#' @rdname run_parallel
#' @export
#'
run_r4ss_parallel <- function(ss_dirlist, ss3_exe = "ss3.exe") {

  #Validate "Directories" in ss_dirlist exists
  checkmate::assert_list(ss_dirlist, any.missing = FALSE)
  checkmate::assert_directory_exists(unlist(ss_dirlist))

  #backend
  progressr::handlers("cli")
  future::plan(multisession, workers = parallelly::availableCores()-2)

  final_results <- run_parallel(ss_dirlist, ss3_exe)

  future::plan(sequential)

  return(final_results)
}

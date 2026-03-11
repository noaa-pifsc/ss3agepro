


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
#' @import progressr
#' @export
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
  checkmate::assert_list(ss_dirlist)

  #Validate "Directories" in ss_dirlist exists
  for(xdir in 1:length(ss_dirlist)){
    checkmate::assert_directory_exists(ss_dirlist[[xdir]])
  }

  #
  progressr::handlers("cli")

  #backend
  future::plan(multisession, workers = future::availableCores()-2)


  parallel_r4ss_run <- function(ss_dirlist) {

    steps <- progressr::progressor(steps = length(ss_dirlist))

    #parallel Loop
    results_list <- future.apply::future_lapply(ss_dirlist, function(x) {

      out <- r4ss::run(dir = x[[1]], exe = ss3_exe, skipfinished = FALSE)

      steps(sprintf("Finished %s", basename(x[[1]] )))

      return(out)

    }, future.seed = TRUE)

    return(results_list)
  }

  final_results <- with_progress({
    parallel_r4ss_run(ss_dirlist)
  })

  future::plan(sequential)

  return(final_results)


  # NumCores <- parallel::detectCores()
  # cl       <- parallel::makeCluster(NumCores-2)
  # parallel::parLapply(cl,ss_dirlist,function(x) {
  #     #Use r4ss::run to validate ss3_exe
  #     r4ss::run(dir = x[[1]], exe = ss3_exe, skipfinished = FALSE)
  # })
  #parallel::stopCluster(cl)

}

# test_storagehub.R
# Author: Emmanuel Blondel <emmanuel.blondel1@gmail.com>
#
# Description: Integration tests for 'D4Science' storage hub interaction
#=======================
require(d4storagehub4R, quietly = TRUE)
require(testthat)

#init
test_that("all Storage hub methods",{
  testthat::skip_on_cran()
  
  STORAGE <- StoragehubManager$new(token = Sys.getenv("D4SCIENCE_TOKEN"), logger = "DEBUG")
  expect_is(STORAGE, "StoragehubManager")
  
  #user profile
  user <- STORAGE$getUserProfile()
  expect_is(user, "list")
  expect_true(all(names(user) %in% c("roles", "context", "avatar", "fullname", "username")))
  expect_equal(user$username, "emmanuel.blondel")
  
  #workspace
  expect_equal(STORAGE$getUserWorkspace(), "/Home/emmanuel.blondel/Workspace")
  expect_equal(STORAGE$getWSRootID(), "c158abd3-89fd-40a8-b3a8-d38da88c355e")
  
  #operations
  #createFolder
  expect_error(STORAGE$createFolder("folder1/subfolder2/subfolder3", name = "d4storagehub4R",  recursive = FALSE))
  created <- STORAGE$createFolder("folder1/subfolder2/subfolder3", name = "d4storagehub4R",  recursive = TRUE)
  expect_is(created, "character")
  folders <- STORAGE$listWSElementsByPath(folderPath = "folder1/subfolder2/subfolder3")
  expect_is(folders, "data.frame")
  expect_equal(folders[1,]$id, created)
  
  #uploadFile
  writeLines("This is a README file", "README.txt")
  fileId <- STORAGE$uploadFile("folder1/subfolder2/subfolder3/d4storagehub4R", file = "README.txt", description = "d4storagehub4R R package README file")
  expect_is(fileId, "character")
  
  #public file link
  fileLink <- STORAGE$getPublicFileLink("folder1/subfolder2/subfolder3/d4storagehub4R/README.md")
  expect_is(fileLink, "character")
  expect_true(startsWith(fileLink, "https://data.d4science.org/shub"))
  download.file(fileLink, destfile = "README_storagehub.txt", mode = "wb")
  source_md5 <- tools::md5sum("README.txt"); names(source_md5) = NULL
  target_md5 <- tools::md5sum("README_storagehub.txt"); names(target_md5) = NULL
  expect_equal(source_md5, target_md5)
  
  #deleteItem
  deleted <- STORAGE$deleteItem(itemPath = "folder1")
  expect_true(deleted)
  
})
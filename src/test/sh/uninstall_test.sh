# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "uninstall.inc"

before () {
  module_depends uninstall
}

it_routes () {
  cl_uninstall () {
    tested=true
  }
  route_perform uninstall
  test "$tested" = "true"
}

it_has_usage () {
  about=`module_annotate uninstall about`
  test "$about" = "remove all installed files"
}

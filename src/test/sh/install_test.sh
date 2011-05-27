# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "install.inc"

before () {
  module_depends install
}

it_routes () {
  cl_install () {
    tested=true
  }
  route_perform install
  test "$tested" = "true"
}

it_has_usage () {
  about=`module_annotate install about`
  test "$about" = "copy all files into place"
}

it_sets_the_install_destination () {
  test "$cl_install_destination" = "$HOME/.cascading.load"
}

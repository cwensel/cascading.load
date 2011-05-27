# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "version.inc"

before () {
  module_exit () {
    module_exited=true
  }
  HADOOP_BIN=echo
  cl_jar_path=/
  module_depends version
}

it_routes () {
  ct_hadoop () {
    hadooped=true
  }
  cl_jar () {
    [ "$hadooped" = "true" ] && jarred=true
  }
  cl_version () {
    [ "$jarred" = "true" ] && tested=true
  }
  route_perform version
  test "$tested" = "true"
}

it_has_usage () {
  about=`module_annotate version about`
  test "$about" = "displays version information"
}

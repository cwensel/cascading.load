# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "doc.inc"

before () {
  HADOOP_BIN=echo
  cl_jar_path=/
  module_depends doc
}

it_prints_an_about_message () {
  about=`module_annotate doc about`
  test "$about" = "describes the Cascading.Load arguments"
}

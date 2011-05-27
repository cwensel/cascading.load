# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "cascading/help.inc"

before () {
  module_depends cascading/help
}

it_routes () {
  ct_help () {
    tested=true
  }
  route_perform help
  test "$tested" = "true"
}

it_has_usage () {
  about=`module_annotate help about`
  test "$about" = "display this screen"
}

it_formats_the_module_list () {
  _MODULE_abouttest=testing
  OUTPUT=`MODULES=test ct_help_module_list`
  test "$OUTPUT" = "  test - testing"
}

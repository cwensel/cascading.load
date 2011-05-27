# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "jar.inc"

before () {
  TMP_HADOOP=`mktemp -dt cl_jar-hadoop-spec.XXXXXX`
  mkdir -p $TMP_HADOOP/bin/
  touch $TMP_HADOOP/bin/hadoop
  HADOOP_HOME=$TMP_HADOOP

  TMP_JAR=`mktemp -dt cl_jar-spec.XXXXXX`
  touch $TMP_JAR/cascading.load-test.jar

  module_depends jar
}

after () {
  rm -rf $TMP_HADOOP $TMP_JAR
}

it_runs_silently_if_cl_jar_path_is_set () {
  cl_jar_path=/
  tested=true

  module_exit () {
    unset tested
  }

  cl_jar
  test "$tested" = "true"
}

it_runs_silently_if_it_finds_multitool_jar () {
  CT_PATH=$TMP_JAR
  tested=true

  module_exit () {
    unset tested
  }

  cl_jar

  test "$tested" = "true"
  test "$cl_jar_path" = "$TMP_JAR/cascading.load-test.jar"
}

it_complains_if_it_cannot_find_multitool_jar () {
  rm $TMP_JAR/cascading.load-test.jar
  CT_PATH=$TMP_JAR

  module_exit () {
    [ "$*" = "cascading.load.jar not found" ] && tested=true
  }

  cl_jar
  test "$tested" = "true"
}

# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "cascading/hadoop.inc"

before () {
  module_depends cascading/hadoop
}

it_detects_hadoop_if_HADOOP_HOME_is_set () {
  temp_hadoop=`mktemp -dt ct_hadoop-spec.XXXXXX`
  mkdir -p $temp_hadoop/bin/
  touch $temp_hadoop/bin/hadoop
  PATH=/usr/bin/:/bin:/usr/sbin:/sbin
  HADOOP_HOME=$temp_hadoop
  tested=true

  module_exit () {
    tested=
  }

  ct_hadoop
  rm -rf $temp_hadoop

  test "$tested" = "true"
}

it_exits_if_hadoop_is_not_in_HADOOP_HOME () {
  HADOOP_HOME=/var
  PATH=/usr/bin/:/bin:/usr/sbin:/sbin

  module_exit () {
    [ "$*" = "HADOOP_HOME is set, but $HADOOP_HOME/bin/hadoop was not found." ] && tested=true
  }

  ct_hadoop
  test "$tested" = "true"
}

it_exits_if_HADOOP_HOME_is_not_set () {
  unset HADOOP_HOME
  PATH=/usr/bin/:/bin:/usr/sbin:/sbin

  module_exit () {
    [ "$*" = "HADOOP_HOME was not set and hadoop is not in your PATH" ] && tested=true
  }

  ct_hadoop
  test "$tested" = "true"
}

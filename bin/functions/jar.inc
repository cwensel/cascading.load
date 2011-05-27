# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log cascading/hadoop

cl_jar ()
{
  if [ -z "$cl_jar_path" ]
  then
    cl_jar_path=`find $CT_PATH -name cascading.load*.jar | head -n1`

    if [ -z "$cl_jar_path" ]
    then
      module_exit "cascading.load.jar not found"
    fi
  fi
}

cl_jar_usage ()
{
  if [ -z "$cl_jar_usage_" ]
  then
    ct_hadoop
    cl_jar
    cl_jar_usage_=`$HADOOP_BIN jar $cl_jar_path | tail -n +3`
  fi

  echo "$cl_jar_usage_"
}

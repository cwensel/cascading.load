# Copyright (c) 2011 Concurrent, Inc.

module_depends cascading/hadoop jar doc

route_default cl_run
route_before cl_run cl_run_error_argless ct_hadoop cl_jar

cl_run_error_argless ()
{
  if [ $# -eq 0 ]
  then
    ct_help
    echo
  fi
}

cl_run ()
{
  if [ $# -eq 0 ]
  then
    module_exit "No arguments specified"
  else
    $HADOOP_BIN jar "$cl_jar_path" $@ 2>&1 | while read line
    do
      if echo $line | grep INFO > /dev/null
      then
        info $line
      elif echo $line | grep WARN > /dev/null
      then
        warn $line
      elif echo $line | grep ERROR > /dev/null
      then
        error $line
      elif echo $line | grep Exception > /dev/null
      then
        stacktrace $line
      elif echo $line | grep "^error: " > /dev/null
      then
        cl_doc_argument_error $line
        module_exit
      elif [ -n "$cl_log_verbose" ]
      then
        log $line
      fi
    done
  fi
}

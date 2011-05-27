# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log core/route jar cascading/help

route_match "^help doc \w+\$" cl_doc
route_before cl_doc ct_hadoop cl_jar

module_annotate doc about "describes a Cascading.Load command"

cl_doc_table ()
{
  names=`cl_jar_usage | tail -n +5 | sed -e 's/^[ \t]*//' | cut -d " " -f1 | sort`
  set -- $names
  while [ $# -gt 0 ]
  do
    printf "  %-25s%-25s%-25s" "$1" "$2" "$3"
    echo
    shift 3
  done
}

module_annotate_block doc usage <<EOF
Examine the available operational parameters of Cascading.Load.

Usage:
  `basename $0` help doc [parameter]

Valid Arguments:
`cl_doc_table`
EOF

cl_doc_argument_usage ()
{
  module_annotate help banner
  echo

  name=$1
  shift
  description=$@

  cat <<-USAGE
$name: $description

Usage:
  cascading.load $name=[value]
USAGE
}

cl_doc_argument_error ()
{
  module_annotate help banner
  module_annotate doc usage | tail -n +5
  echo
  error `echo $@ | sed s/error:/ERROR/`
}

cl_doc ()
{
  request=`cl_jar_usage | egrep "^  $2 "`
  if [ -n "$request" ]
  then
    cl_doc_argument_usage $request
  else
    cl_doc_argument_error "ERROR Invalid argument: $2"
    module_exit
  fi
}

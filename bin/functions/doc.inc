# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log core/route jar cascading/help

route_match "^help doc\$" cl_doc
route_before cl_doc ct_hadoop cl_jar

module_annotate doc about "describes the Cascading.Load arguments"

cl_doc_table ()
{
  cl_jar_usage | tail -n +5
}

module_annotate_block doc usage <<EOF
Examine the available operational parameters of Cascading.Load.

Usage:
  `basename $0` [argument] ...

Valid Arguments:
`cl_doc_table`
EOF

cl_doc_argument_error ()
{
  module_annotate help banner
  module_annotate doc usage | tail -n +5
  echo
  error `echo $@ | sed s/error:/ERROR/`
}

cl_doc ()
{
  module_annotate help banner
  module_annotate doc usage | tail -n +5
}

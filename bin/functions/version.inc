# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log core/route cascading/hadoop jar

route_match "^version|-v|--version$" cl_version
route_before cl_version ct_hadoop cl_jar

module_annotate version about "displays version information"
module_annotate_block version usage <<USAGE
Prints version information about Cascading and Cascading.Load.

Usage:
  `basename $0` version
USAGE

cl_version ()
{
  cat <<VERSION
Cascading.Load: $cl_jar_path
`cl_jar_usage | head -n2`
VERSION
}

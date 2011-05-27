# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log core/route

route_match '^-h|--help|(help(\s\w+)?)' ct_help

module_annotate help about "display this screen"
module_annotate_block help usage <<-USAGE
Runs with the provided parameters.

Usage:
  `basename $0` [flags] action [action-flags]
  `basename $0` help [action]

Flags:
  -h|--help      - displays this message
  -q|--quiet     - silences INFO messages
  -nc|--no-color - disables colorization of messages

Actions:
USAGE

module_annotate_block help banner <<-USAGE
`basename $0`
Copyright (c) `date +%Y` Concurrent, Inc. (http://concurrentinc.com)
USAGE

ct_help_module_list ()
{
  local longest=0
  local module
  local about

  for module in $MODULES; do
    module=`basename $module`
    local length=`expr $module : '.*'`
    
    if [ $length -gt $longest ]; then
      longest=$length
    fi
  done

  for module in `echo $MODULES | tr " " "\n" | sort | tr "\n" " "`
  do
    module=`basename $module`

    if [ "$module" != "run" ]
    then
      about=`module_annotate $module about`

      if [ -n "$about" ]
      then
        printf "  %-${longest}s - " "$module"
        echo "$about"
      fi
    fi
  done
}

ct_help ()
{
  module_annotate help banner
  echo

  if [ $# -eq 0 ]; then
    module_annotate help usage | tail -n +3
    ct_help_module_list
  else
    while [ $# -gt 0 ]; do
      module=`module_annotate $1 usage`

      if [ -n "$module" ] && [ "$1" != "run" ]; then
        echo "$1: $module"

        if [ "$1" = "help" ]; then
          ct_help_module_list
        fi
      else
        echo "Valid Topics:"
        ct_help_module_list
        echo
        module_exit "Invalid help topic: $1"
      fi
      shift
    done
  fi
}

# Copyright (c) 2011 Concurrent, Inc.

if [ -z "$MODULES_ROOT" ]
then  
  MODULES_ROOT=`dirname $0`
  MODULES_ROOT=`cd $MODULES_ROOT && cd .. && pwd`
fi

MODULES=

module_depends ()
{
  for dep in "$@"
  do
    for extant in $MODULES; do
      if [ "$dep" = "$extant" ]; then
        break 1
      fi
    done

    if [ "$dep" != "$extant" ]; then
      MODULES="$MODULES$dep "
      local module=$MODULES_ROOT/$dep.inc

      if [ -f $module ]; then
        . $module
      fi
    fi
  done
}

module_annotate ()
{
  local module=$1
  local aspect=$2
  shift 2
  local message="$@"

  if [ -n "$module" ] && [ -n "$aspect" ]
  then
    if [ $# -gt 0 ]
    then
      eval _MODULE_$aspect$module=\"\$message\"
    else
      eval echo \"\$_MODULE_$aspect$module\"
    fi
  fi
}

module_annotate_block ()
{
  local module=$1
  local aspect=$2

  if [ -n "$module" ]
  then
    eval _MODULE_$aspect$module=\$\(cat\)
  fi
}

module_exit ()
{
  local code=1

  if [ $# -gt 0 ] && echo $1 | grep "^[0-9][0-9]*$" 2>&1 > /dev/null
  then
    code=$1
    shift 1
  fi

  if [ $# -gt 0 ]
  then
    echo "* $@"
  fi

  exit $code
}

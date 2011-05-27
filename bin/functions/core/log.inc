# Copyright (c) 2011 Concurrent, Inc.

LOG_STACK=0

if  [ "$LOG_COLOR" = "always" ] || ( test -t 1 )
then
  LOG_COLOR_RED=$(printf "\033[31m")
  LOG_COLOR_GREEN=$(printf "\033[32m")
  LOG_COLOR_YELLOW=$(printf "\033[33m")
  LOG_COLOR_BLUE=$(printf "\033[34m")
  LOG_COLOR_RESET=$(printf "\033[m")
fi

LOG_PATTERN_HILITE="s/INFO/${LOG_COLOR_GREEN}INFO${LOG_COLOR_BLUE}/"
LOG_PATTERN_INFO="s/INFO/${LOG_COLOR_GREEN}INFO${LOG_COLOR_RESET}/"
LOG_PATTERN_WARN="s/WARN/${LOG_COLOR_YELLOW}WARN${LOG_COLOR_RESET}/"
LOG_PATTERN_ERROR="s/ERROR/${LOG_COLOR_RED}ERROR${LOG_COLOR_RESET}/"
LOG_HILITE_STRINGS="cascade flow"

log ()
{
  [ "$LOG_STACK" -gt 0 ] && printf '\t%.0s' {1..$LOG_STACK}
  echo "$@"
}

info ()
{
  LOG_STACK=0
  if [ -n "$LOG_VERBOSE" ]
  then
    if [ -n "$LOG_COLOR" ]
    then
      for hilite in $LOG_HILITE_STRINGS
      do
        if echo "$@" | grep "INFO $hilite" > /dev/null
        then
          echo `echo "$@" | sed $LOG_PATTERN_HILITE`$LOG_COLOR_RESET
          return
        fi
      done

      echo `echo "$@" | sed $LOG_PATTERN_INFO`$LOG_COLOR_RESET
    else
      echo "$@"
    fi
  fi
}

warn ()
{
  LOG_STACK=0
  if [ -n "$LOG_COLOR" ]
  then
    echo `echo "$@" | sed $LOG_PATTERN_WARN`$LOG_COLOR_RESET
  else
    echo "$@"
  fi
}

error ()
{
  LOG_STACK=0
  if [ -n "$LOG_COLOR" ]
  then
    echo `echo "$@" | sed $LOG_PATTERN_ERROR`$LOG_COLOR_RESET
  else
    echo "$@"
  fi
}

stacktrace ()
{
  log $@
  LOG_STACK=$(($LOG_STACK + 1))
}

# Override module#exit
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
    error "ERROR $@"
  fi

  exit $code
}
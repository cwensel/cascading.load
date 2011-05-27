# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "core/log.inc"

before () {
  LOG_COLOR=always
  LOG_VERBOSE=true
  module_depends core/log
}

it_logs_a_message() {
  LOG_INPUT='this has been logged'
  LOG_OUTPUT=`log $LOG_INPUT`
  test "$LOG_OUTPUT" = "$LOG_INPUT"
}

it_colorizes_an_info_message() {
  LOG_INPUT='this is information'
  LOG_OUTPUT=`info "INFO $LOG_INPUT"`

  test "$LOG_OUTPUT" = "${LOG_COLOR_GREEN}INFO$LOG_COLOR_RESET $LOG_INPUT$LOG_COLOR_RESET"
}

it_will_not_log_info_messages_without_verbose () {
  unset LOG_VERBOSE
  LOG_OUTPUT=`info "INFO won't show up"`
  
  test "$LOG_OUTPUT" = ""
}

it_colorizes_a_hilite_message() {
  LOG_HILITE_STRINGS='hilite_this'
  LOG_INPUT=$LOG_HILITE_STRINGS
  LOG_OUTPUT=`info "INFO $LOG_INPUT"`
  
  test "$LOG_OUTPUT" = "${LOG_COLOR_GREEN}INFO$LOG_COLOR_BLUE $LOG_INPUT$LOG_COLOR_RESET"
}

it_colorizes_a_warning_message() {
  LOG_INPUT='deprecated'
  LOG_OUTPUT=`warn "WARN $LOG_INPUT"`

  test "$LOG_OUTPUT" = "${LOG_COLOR_YELLOW}WARN$LOG_COLOR_RESET $LOG_INPUT$LOG_COLOR_RESET"
}

it_colorizes_an_error_message() {
  LOG_INPUT='syntax error'
  LOG_OUTPUT=`error "ERROR $LOG_INPUT"`

  test "$LOG_OUTPUT" = "${LOG_COLOR_RED}ERROR$LOG_COLOR_RESET $LOG_INPUT$LOG_COLOR_RESET"
}

it_indents_a_stacktrace() {
  LOG_INPUT='some stuff'
  
  stacktrace
  LOG_OUTPUT=`stacktrace "$LOG_INPUT"`
  test "$LOG_OUTPUT" = "	$LOG_INPUT"
}

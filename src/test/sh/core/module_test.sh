# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "core/module.inc"

it_imports_a_module () {
  test "$MODULES" = ""
  module_depends core/log
  test "$MODULES" = "core/log "
}

it_does_not_import_a_module_twice () {
  test "$MODULES" = ""
  module_depends core/route
  test "$MODULES" = "core/route "
  module_depends core/route
  test "$MODULES" = "core/route "
}

it_quietly_ignores_invalid_modules () {
  OUTPUT=`module_depends _bad_`
  test "$OUTPUT" = ""
}

it_annotates_modules () {
  input="to be annotated"
  module_annotate _to_annotate_ longer_name "$input"
  test "$_MODULE_longer_name_to_annotate_" = "$input"

  OUTPUT=`module_annotate _to_annotate_ longer_name`
  test "$OUTPUT" = "$input"
}

it_annotates_modules_with_a_heredoc () {
  input="to be annotated"
  module_annotate_block _to_annotate_ longer_name <<USAGE
`echo $input`
USAGE

  test "$_MODULE_longer_name_to_annotate_" = "$input"
}

it_exits () {
  do_exit () {
    test "$?" = "1"
    exit 0
  }
  trap do_exit EXIT
  module_exit
}

it_exits_with_a_message () {
  do_exit () {
    test "$?" = "1"
    test "$OUTPUT" = "* foo bar"
    exit 0
  }
  trap do_exit EXIT
  OUTPUT=`module_exit foo bar`
}

it_exits_with_a_code () {
  do_exit () {
    test "$?" = "66"
    test "$OUTPUT" = ""
    exit 0
  }
  trap do_exit EXIT
  OUTPUT=`module_exit 66`
}

it_exits_with_a_code_and_message () {
  do_exit () {
    test "$?" = "66"
    test "$OUTPUT" = "* no problems"
    exit 0
  }
  trap do_exit EXIT
  OUTPUT=`module_exit 66 no problems`
}

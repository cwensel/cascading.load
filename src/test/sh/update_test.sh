# Copyright (c) 2011 Concurrent, Inc.

. `dirname $(cd ${0%/*} && echo $PWD/${0##*/})`/include.sh

describe "update.inc"

before () {
  CURL_BIN="echo"
  module_depends update
}

it_routes () {
  cl_update_reject_git () {
    tested=1
  }
  cl_update_parse_latest () {
    [ "$tested" = "1" ] && tested=2
  }
  cl_update_create_temp () {
    [ "$tested" = "2" ] && tested=3
  }
  cl_update () {
    [ "$tested" = "3" ] && tested=4
  }
  
  route_perform update
  test "$tested" = "4"
}

it_exits_if_a_git_repo_is_detected () {
  temp_update=`mktemp -dt cl_update-spec.XXXXXX`
  mkdir -p $temp_update/.git
  CT_PATH=$temp_update

  module_exit () {
    [ "$*" = "$CT_PATH is a git repository.  Use git pull to update." ] && tested=1
  }
  cl_update_parse_latest () {
    [ "$tested" = "1" ] && tested=2
  }
  cl_update_create_temp () {
    [ "$tested" = "2" ] && tested=3
  }
  cl_update () {
    [ "$tested" = "3" ] && tested=4
  }

  route_perform update
  rm -rf $temp_update

  test "$tested" = "4"
}

it_parses_the_latest_load_location () {
  testing_url="http://files.cascading.org/cascading.load/cascading.load-latest.tgz"
  CURL_BIN="echo $testing_url"
  cl_update_reject_git () {
    CT_PATH=/does/not/exist
  }
  cl_update_create_temp () {
    tested=1
  }
  cl_update () {
    [ "$cl_update_latest" = "$testing_url" ] && [ "$tested" = "1" ] && tested=2
  }

  route_perform update
  test "$tested" = "2"
}

it_complains_if_curl_fails_to_fetch_latest () {
  CURL_BIN=echo
  cl_update_reject_git () {
    CT_PATH=/does/not/exist
  }
  module_exit () {
    [ "$*" = "Cannot get latest cascading.load from http://files.cascading.org/cascading.load/cascading.load-current.txt" ] && tested=1
  }
  cl_update_create_temp () {
    [ "$tested" = "1" ] && tested=2
  }
  cl_update () {
    [ "$tested" = "2" ] && tested=3
  }

  route_perform update
  test "$cl_update_latest" = "$testing_url" -a "$tested" = "3"
}

it_allows_a_version_specifier () {
  cl_update_reject_git () {
    CT_PATH=/does/not/exist
  }
  cl_update_create_temp () {
    temped=true
  }
  cl_update () {
    tested=true
  }

  route_perform update -v latest
  test "$cl_update_latest" = "http://files.cascading.org/cascading.load/cascading.load-latest.tgz"

  route_perform update --version more_latest
  test "$cl_update_latest" = "http://files.cascading.org/cascading.load/cascading.load-more_latest.tgz"

  route_perform update --version=super_latest
  test "$cl_update_latest" = "http://files.cascading.org/cascading.load/cascading.load-super_latest.tgz"
}

it_updates_an_existing_installation () {
  temp_update=`mktemp -dt cl_update-spec.XXXXXX`
  mkdir $temp_update/mt
  CT_PATH=$temp_update/mt
  touch $CT_PATH/foo

  cl_update_temp=$temp_update
  cl_update_temp_tgz=$cl_update_temp/current.tgz
  cl_update_temp_new=$cl_update_temp/new
  cl_update_temp_old=$cl_update_temp/old

  cl_update_curl () {
    touch $cl_update_temp_tgz
  }
  tar () {
    mkdir $4/stuff
    touch $4/stuff/bar
  }

  test -e "$CT_PATH/foo"
  test ! -e "$CT_PATH/bar"

  cl_update

  test ! -e "$CT_PATH/foo"
  test -e "$CT_PATH/bar"
  test -e "$cl_update_temp_old/foo"

  rm -rf $temp_update
}

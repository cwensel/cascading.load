# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log core/route

route_match "^update|update (-v|--version(=|\s))\d+\$" cl_update
route_before cl_update cl_update_reject_git cl_update_parse_latest cl_update_create_temp

module_annotate update about "update Cascading.Load"
module_annotate_block update usage <<USAGE
Update Cascading.Load from the source.

Usage:
  update [flags]

Flags:
  -v|--version - specify the version of Cascading.Load to fetch
USAGE

cl_update_base_url="http://files.cascading.org/cascading.load"

cl_update_create_temp ()
{
  cl_update_temp=`mktemp -dt cascading_load-cl_update.XXXXX`

  if [ ! -d "$cl_update_temp" ]
  then
    module_exit "Could not create temporary directory"
  fi

  cl_update_temp_tgz=$cl_update_temp/current.tgz
  cl_update_temp_new=$cl_update_temp/new
  cl_update_temp_old=$cl_update_temp/old
}

cl_update_curl ()
{
  if [ -z "$CURL_BIN" ]
  then
    CURL_BIN=`which curl 2>/dev/null || echo`

    if [ -z "$CURL_BIN" ]
    then
      module_exit "curl was not found in PATH"
    fi
  fi
  
  if [ -n "$cl_log_verbose" ]
  then
    $CURL_BIN --progress-bar $@
  else
    $CURL_BIN -s $@
  fi
}

cl_update_reject_git ()
{
  if [ -d "$CT_PATH/.git" ]
  then
    module_exit "$CT_PATH is a git repository.  Use git pull to update."
  fi
}

cl_update_parse_latest ()
{
  while [ $# -gt 0 ]
  do
    case "$1" in
      --version=*)
        cl_update_latest=$cl_update_base_url/cascading.load-`echo $1 | sed 's/--version=\(.*\)/\1/g'`.tgz
        ;;
      -v|--version)
        cl_update_latest=$cl_update_base_url/cascading.load-$2.tgz
        break 1
        ;;
      *)
        module_exit "Invalid option $1"
        ;;
    esac
    shift
  done

  if [ -z "$cl_update_latest" ]
  then
    info "INFO Parsing version number from $cl_update_base_url/cascading.load-current.txt"
    cl_update_latest=`cl_update_curl $cl_update_base_url/cascading.load-current.txt | grep "$cl_update_base_url.*\.tgz" | sed 's|^\([^ ]*://[^ ]*\).*|\1|g' | head -n1`
  fi

  if [ -z "$cl_update_latest" ]
  then
    module_exit "Cannot get latest cascading.load from $cl_update_base_url/cascading.load-current.txt"
  fi
}

cl_update ()
{
  info "INFO Fetching $cl_update_latest"
  cl_update_curl -o $cl_update_temp_tgz $cl_update_latest

  if [ $? -ne 0 ]
  then
    rm -rf $cl_update_temp
    module_exit "Could not fetch $cl_update_latest"
  fi

  mkdir -p $cl_update_temp_new

  info "INFO Extracting $cl_update_temp_tgz"
  tar zxf $cl_update_temp_tgz -C $cl_update_temp_new

  info "INFO Backing $CT_PATH up to $cl_update_temp_old"
  mv $CT_PATH $cl_update_temp_old

  for listing in `ls $cl_update_temp_new`
  do
    listing=`cd $cl_update_temp_new && pwd`/$listing
    if [ -d "$listing" ]
    then
      info "INFO Copying $listing to $CT_PATH"
      cp -Rf $listing $CT_PATH
      break 1
    fi
  done

  warn "WARN Remember to remove $cl_update_temp"
}

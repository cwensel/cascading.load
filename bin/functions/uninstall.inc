# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log install

route_match "^uninstall" cl_uninstall

module_annotate uninstall about "remove all installed files"
module_annotate_block uninstall usage <<-USAGE
Removes all the installed files.

Usage:
  `basename $0` uninstall [options]

Options:
  -d|--destination     - specifies the install destination
                         defaults to /usr/local/lib/cascading.load (as root/sudo)
                         defaults to $HOME/.cascading.load (as user)
  -b|--bin-destination - specifies the destination for executable files
                         defaults to /usr/local/bin
                         (only applies when running as root/sudo)
USAGE

cl_uninstall ()
{
  if [ -d "$cl_install_destination" ]
  then
    info "INFO Removing $cl_install_destination"
    rm -rf $cl_install_destination
  fi

  if [ -n "$cl_install_bin_destination" ]
  then
    for link in $cl_install_bin_links
    do
      binfile=$cl_install_bin_destination/$link

      if [ -h "$binfile" ]
      then
        info "INFO Unlinking $binfile"
        rm -f $binfile
      fi
    done
  fi
}

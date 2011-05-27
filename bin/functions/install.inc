# Copyright (c) 2011 Concurrent, Inc.

module_depends core/log core/route jar

route_match "^install" cl_install

module_annotate install about "copy all files into place"
module_annotate_block install usage <<-USAGE
Copies all necessary files into place.

Usage:
  `basename $0` install [options]

Options:
  -d|--destination     - specifies the install destination
                         defaults to /usr/local/lib/cascading.load (as root/sudo)
                         defaults to $HOME/.cascading.load (as user)
  -b|--bin-destination - specifies the destination for executable files
                         defaults to /usr/local/bin
                         (only applies when running as root/sudo)
USAGE

cl_install_dirs="bin"
cl_install_bin="bin/cascading.load"
cl_install_bin_links="cascading.load"
cl_install_files="README.txt LICENSE.txt"

if [ "$UID" = "0" ]
then
  cl_install_destination=/usr/local/lib/cascding.load
  cl_install_bin_destination=/usr/local/bin
else
  cl_install_destination=$HOME/.cascading.load
  cl_install_bin_destination=
fi

cl_install_examine_hadoop ()
{
  module_exit ()
  {
    error ERROR $@
  }

  ct_hadoop
  cl_jar
}

cl_install ()
{
  mkdir -p $cl_install_destination

  for dir in $cl_install_dirs
  do
    info "INFO Installing $cl_install_destination/$dir/*"
    cp -Rf $CT_PATH/$dir $cl_install_destination
  done

  for file in $cl_install_files
  do
    info "INFO Installing $cl_install_destination/$file"
    cp -f $CT_PATH/$file $cl_install_destination
  done

  cl_install_examine_hadoop

  if [ -n "$cl_jar_path" ]
  then
    info "INFO Installing $cl_install_destination/`basename $cl_jar_path`"
    cp -f $cl_jar_path $cl_install_destination/`basename $cl_jar_path`
  fi

  if [ "$UID" = "0" ]
  then
    chmod +x $cl_install_destination/$cl_install_bin
    mkdir -p $cl_install_bin_destination

    for link in $cl_install_bin_links
    do
      info "INFO Linking $cl_install_destination/$cl_install_bin -> $cl_install_bin_destination/$link"
      ln -sf $cl_install_destination/$cl_install_bin $cl_install_bin_destination/$link
    done
  elif ! echo $PATH | grep "$cl_install_destination/bin" > /dev/null
  then
    warn "WARN You should add $cl_install_destination/bin to your \$PATH."
  fi
}

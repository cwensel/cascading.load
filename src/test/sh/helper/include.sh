# Copyright (c) 2011 Concurrent, Inc.

if [ -z "$CT_PATH" ]
then
  CT_PATH=`dirname $0`/../../../..
  CT_PATH=`cd $CT_PATH && pwd`
fi

MODULES_ROOT=$CT_PATH/bin/functions
. $MODULES_ROOT/core/module.inc

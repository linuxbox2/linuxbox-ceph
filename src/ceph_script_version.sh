#!/bin/sh
echo "#!/usr/bin/env python" >$2
grep "#define CEPH_GIT_NICE_VER" $3 | \
sed -e 's/#define \(.*VER\) /\1=/' >>$2
grep "#define CEPH_GIT_VER" $3 | \
sed -e 's/#define \(.*VER\) /\1=/' -e 's/=\(.*\)/="\1"/' >>$2
cat $1 >>$2



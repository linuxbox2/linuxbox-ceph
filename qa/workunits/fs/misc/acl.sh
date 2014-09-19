#!/bin/sh -x

set -e
mkdir -p testdir
cd testdir

set +e
setfacl -d -m u:nobody:rw .
if test $? != 0; then
	echo "Filesystem does not support ACL"
	exit 0
fi

set -e
for (( i=0; i<100; i++))
do
	# inherited ACL from parent directory's default ACL
	mkdir d1
	c1=`getfacl d1 | grep -c "nobody:rw"`
	sudo echo 3 > /proc/sys/vm/drop_caches
	c2=`getfacl d1 | grep -c "nobody:rw"`
	rmdir d1
	if [[ $c1 -ne 2 || $c2 -ne 2 ]]
	then
		echo "ERROR: incorrect ACLs"
		exit 1
	fi
done

cd ..
rmdir testdir
echo OK

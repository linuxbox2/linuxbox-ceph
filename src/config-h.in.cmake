/* config.h file expanded by Cmake for build */

#ifndef CONFIG_H
#define CONFIG_H

/* fallocate(2) is supported */
#cmakedefine CEPH_HAVE_FALLOCATE 

/* Define if darwin/osx */
#cmakedefine DARWIN 

/* Define if you want C_Gather debugging */
#cmakedefine DEBUG_GATHER 1

/* Define if enabling coverage. */
#cmakedefine ENABLE_COVERAGE

/* FastCGI headers are in /usr/include/fastcgi */
#cmakedefine FASTCGI_INCLUDE_DIR

/* Define to 1 if you have the <arpa/inet.h> header file. */
#cmakedefine HAVE_ARPA_INET_H 1

/* Define to 1 if you have the <dirent.h> header file, and it defines `DIR'.
   */
#cmakedefine HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#cmakedefine HAVE_DLFCN_H 1

/* linux/fiemap.h was found, fiemap ioctl will be used */
#cmakedefine HAVE_FIEMAP_H 1

/* Define to 1 if you have the `fuse_getgroups' function. */
#cmakedefine HAVE_FUSE_GETGROUPS 1

/* Define to 1 if you have the <inttypes.h> header file. */
#cmakedefine HAVE_INTTYPES_H 1

/* Defined if LevelDB supports bloom filters */
#cmakedefine HAVE_LEVELDB_FILTER_POLICY

/* Defined if you don't have atomic_ops */
#cmakedefine HAVE_LIBAIO

/* Define if you have fuse */
#cmakedefine HAVE_LIBFUSE

/* Define to 1 if you have the `leveldb' library (-lleveldb). */
#cmakedefine HAVE_LIBLEVELDB 1

/* Define to 1 if you have the `profiler' library (-lprofiler). */
#cmakedefine HAVE_LIBPROFILER 1

/* Define to 1 if you have the `snappy' library (-lsnappy). */
#cmakedefine HAVE_LIBSNAPPY 1

/* Define if you have tcmalloc */
#cmakedefine HAVE_LIBTCMALLOC

/* Define to 1 if you have the <memory.h> header file. */
#cmakedefine HAVE_MEMORY_H 1

/* Define to 1 if you have the <ndir.h> header file, and it defines `DIR'. */
#cmakedefine HAVE_NDIR_H 1

/* Define to 1 if you have the <netdb.h> header file. */
#cmakedefine HAVE_NETDB_H 1

/* Define to 1 if you have the <netinet/in.h> header file. */
#cmakedefine HAVE_NETINET_IN_H 1

/* Define if you have perftools profiler enabled */
#cmakedefine HAVE_PROFILER

/* Define to 1 if you have the <stdint.h> header file. */
#cmakedefine HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#cmakedefine HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#cmakedefine HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#cmakedefine HAVE_STRING_H 1

/* sync_file_range(2) is supported */
#cmakedefine HAVE_SYNC_FILE_RANGE

/* Define to 1 if you have the <syslog.h> header file. */
#cmakedefine HAVE_SYSLOG_H 1

/* Define to 1 if you have the <sys/file.h> header file. */
#cmakedefine HAVE_SYS_FILE_H 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#cmakedefine HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#cmakedefine HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/ndir.h> header file, and it defines `DIR'.
   */
#cmakedefine HAVE_SYS_NDIR_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#cmakedefine HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#cmakedefine HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/statvfs.h> header file. */
#cmakedefine HAVE_SYS_STATVFS_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#cmakedefine HAVE_SYS_STAT_H 1

/* we have syncfs */
#cmakedefine HAVE_SYS_SYNCFS

/* Define to 1 if you have the <sys/time.h> header file. */
#cmakedefine HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#cmakedefine HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#cmakedefine HAVE_SYS_VFS_H 1

/* Defined if you do not have atomic_ops */
#cmakedefine NO_ATOMIC_OPS

/* Defined if you want pg ref debugging */
#cmakedefine PG_DEBUG_REFS

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
#cmakedefine PTHREAD_CREATE_JOINABLE

/* Define if using CryptoPP. */
#cmakedefine USE_CRYPTOPP

/* Define if using NSS. */
#cmakedefine USE_NSS

/* define if radosgw enabled */
#cmakedefine WITH_RADOSGW

#endif /* CONFIG_H */

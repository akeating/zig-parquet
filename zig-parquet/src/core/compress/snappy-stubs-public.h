// Generated snappy-stubs-public.h for zig-parquet
// This file provides the configuration defines needed by snappy.

#ifndef THIRD_PARTY_SNAPPY_OPENSOURCE_SNAPPY_STUBS_PUBLIC_H_
#define THIRD_PARTY_SNAPPY_OPENSOURCE_SNAPPY_STUBS_PUBLIC_H_

#include <cstddef>

#if defined(__unix__) || defined(__APPLE__)
#define HAVE_SYS_UIO_H 1
#include <sys/uio.h>
#else
#define HAVE_SYS_UIO_H 0
#endif

#define SNAPPY_MAJOR 1
#define SNAPPY_MINOR 2
#define SNAPPY_PATCHLEVEL 2
#define SNAPPY_VERSION \
    ((SNAPPY_MAJOR << 16) | (SNAPPY_MINOR << 8) | SNAPPY_PATCHLEVEL)

namespace snappy {

#if !HAVE_SYS_UIO_H
// Windows does not have an iovec type, yet the concept is universally useful.
struct iovec {
  void* iov_base;
  size_t iov_len;
};
#endif

}  // namespace snappy

#endif  // THIRD_PARTY_SNAPPY_OPENSOURCE_SNAPPY_STUBS_PUBLIC_H_

/*
 * diskstream_util.hpp
 *
 *  Created on: Jun 24, 2013
 *      Author: jinliang
 */

#ifndef __DISKSTREAM_UTIL_HPP__
#define __DISKSTREAM_UTIL_HPP__

#include <sys/time.h>
#include <stddef.h>
#include <stdint.h>

namespace diskstream {

  typedef struct timeval timeval_t;

  int32_t get_micro_second(timeval_t tv);

  // return starting time in microseconds
  int32_t timer_st();

  // return ending time in microseconds
  int32_t timer_ed(int32_t stms);

  void display_errno();

}




#endif /* DISKSTREAM_UTIL_HPP_ */

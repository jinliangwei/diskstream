
#include "diskstream_util.hpp"
#include <errno.h>
#include <iostream>

namespace diskstream {
  int32_t get_micro_second(timeval_t tv){
    return (int32_t) tv.tv_sec*1000000 + tv.tv_usec;
  }

  int32_t timer_st(){
    timeval_t t;
    gettimeofday(&t, NULL);
    return get_micro_second(t);
  }

  int32_t timer_ed(int32_t stms){
    timeval_t t;
    gettimeofday(&t, NULL);
    return get_micro_second(t) - stms;
  }
  void display_errno(){
    switch(errno){
    case EAGAIN:
      std::cout << "error: EAGAIN" << std::endl;
      break;
    case EBADF:
      std::cout << "error: EBADF" << std::endl;
      break;
    case EFBIG:
      std::cout << "error: EFBIG" << std::endl;
      break;
    case EINTR:
      std::cout << "error: EINTR" << std::endl;
      break;
    case EINVAL:
      std::cout << "error: EINVAL" << std::endl;
      break;
    case EIO:
      std::cout << "error: EIO" << std::endl;
      break;
    case EFAULT:
      std::cout << "error: EFAULT" << std::endl;
      break;
    default:
      std::cout << "error: others" << std::endl;
      break;
    }
  }
}





#ifndef __DISKSTREAM_LDA_TASKLOCATOR_HPP__
#define __DISKSTREAM_LDA_TASKLOCATOR_HPP__

#include <stdint.h>

#include "diskstream_buffer.hpp"

namespace diskstream {
  class UniSizeTaskLocator {
  private:
    Buffer **taskbuffers;
    int32_t num_task_buffs;
    int32_t num_my_task_buffs;
    int32_t taskid_st;
    int32_t taskid_ed;
    int32_t buffsize;
    int32_t tasksize;
    int32_t num_task_per_buff;
  public:
    UniSizeTaskLocator();
    ~UniSizeTaskLocator();
    int init(int32_t _num_task_buffs, int32_t _buffsize, int32_t _tasksize);
    // _taskid_st should be the intended first task id
    int reset_taskid_st(int32_t _taskid_st);
    int add_task_buff(Buffer *_buff);
    uint8_t *get_task_ptr(int32_t _taskid);
    int get_range(int32_t &_st, int32_t &_ed);
  };
}

#endif /* DISKSTREAM_LDA_TASKLOCATOR_HPP_ */

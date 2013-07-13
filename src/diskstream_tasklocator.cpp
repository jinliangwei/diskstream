
#include <stddef.h>
#include <stdint.h>
#include <new>
#include "diskstream_tasklocator.hpp"


namespace diskstream {
  UniSizeTaskLocator::UniSizeTaskLocator():
      taskbuffers(NULL),
      num_task_buffs(0),
      num_my_task_buffs(0),
      taskid_st(0),
      taskid_ed(-1),
      buffsize(0),
      tasksize(0),
      num_task_per_buff(0){}

  UniSizeTaskLocator::~UniSizeTaskLocator(){
    if(taskbuffers != NULL){
      delete[] taskbuffers;
    }
  }

  int UniSizeTaskLocator::init(int32_t _num_task_buffs, int32_t _buffsize, int32_t _tasksize){
    num_task_buffs = _num_task_buffs;
    buffsize = _buffsize;
    tasksize = _tasksize;
    num_task_per_buff = buffsize/tasksize;
    try{
      taskbuffers = new Buffer *[num_task_buffs];
    }catch(std::bad_alloc &ba){
      return -1;
    }
    return 0;
  }

  int UniSizeTaskLocator::reset_taskid_st(int32_t _taskid_st){
    taskid_st = _taskid_st;
    taskid_ed = _taskid_st - 1;
    num_my_task_buffs = 0;
    return 0;
  }

  int UniSizeTaskLocator::add_task_buff(Buffer *buff){
    if(num_my_task_buffs >= num_task_buffs) return -1;
    taskbuffers[num_my_task_buffs] = buff;
    ++num_my_task_buffs;
    taskid_ed += buff->get_num_data();
    return 0;
  }

  uint8_t *UniSizeTaskLocator::get_task_ptr(int32_t _taskid){
    if(taskid_st > taskid_ed) return NULL;
    if(_taskid < taskid_st || _taskid > taskid_ed) return NULL;
    int32_t diff = _taskid - taskid_st;
    int32_t buff_idx = diff/num_task_per_buff;
    int32_t buff_offset = diff%num_task_per_buff;
    uint8_t *dptr = taskbuffers[buff_idx]->get_dataptr();
    dptr += buff_offset*tasksize;
    return dptr;
  }

  int UniSizeTaskLocator::get_range(int32_t &_st, int32_t &_ed){
    _st = taskid_st;
    _ed = taskid_ed;
    return 0;
  }
}


#include "diskstream_program.hpp"

namespace diskstream {

  DiskStreamProgram::DiskStreamProgram():
    cnt(0),
    cnt2(0){}

  DiskStreamProgram::~DiskStreamProgram(){}

  int DiskStreamProgram::partial_reduce(uint8_t *task, uint8_t *data, uint8_t *globdata,
                                        int32_t niter, int32_t task_idx){
    ++cnt;
    return 0;
  }

  int DiskStreamProgram::summarize(uint8_t *task){
    ++cnt2;
    return 0;
  }

  int DiskStreamProgram::end_iteration(){
    return 0;
  }

  int64_t DiskStreamProgram::get_cnt(){
    return cnt;
  }

}



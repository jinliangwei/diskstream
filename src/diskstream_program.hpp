
#ifndef __DISKSTREAM_PROGRAM_HPP__
#define __DISKSTREAM_PROGRAM_HPP__

#include <stdint.h>

namespace diskstream {
  class DiskStreamProgram {
  private:
    int64_t cnt;
    int64_t cnt2;
  public:
    DiskStreamProgram();
    virtual ~DiskStreamProgram();

    virtual int partial_reduce(uint8_t *task, uint8_t *data, uint8_t *globdata, int32_t niter,
                       int32_t task_idx); // task_idx is the idx of the task in the data's task list

    virtual int summarize(uint8_t *task);

    virtual int end_iteration();

    int64_t get_cnt();
  };

}




#endif /* DISKSTREAM_PROGRAM_HPP_ */

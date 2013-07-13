
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
    ~DiskStreamProgram();

    int partial_reduce(uint8_t *task, uint8_t *data);

    int summarize(uint8_t *task);

    int end_iteration();

    int64_t get_cnt();
  };

}




#endif /* DISKSTREAM_PROGRAM_HPP_ */

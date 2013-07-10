/*
 * diskstream_lda_executor.hpp
 *
 *  Created on: Jul 4, 2013
 *      Author: jinliang
 */

#ifndef __DISKSTREAM_LDA_EXECUTOR_HPP__
#define __DISKSTREAM_LDA_EXECUTOR_HPP__

#include <stdint.h>
#include <zmq.hpp>
#include <string>
#include <vector>
#include <assert.h>
#include <string>

#include "../diskstream_buffermanager.hpp"
#include "../diskstream_buffer.hpp"
#include "../diskstream_iothread.hpp"
#include "../util/diskstream_util.hpp"
#include "../diskstream_types.hpp"

#include "lda_datastruct.hpp"

namespace diskstream {
  static const std::string KDiskIOEndp = "inproc://ldaexecutor_diskio_endp";
  static const int32_t KMinTaskBuffs = 4;
  static const int32_t KMinDataBuffs = 4;
  static const int32_t KNumExternDataBuffs = 2;
  // mininum number of buffers to be held by BufferManager during init phase to ensure I/O
  // efficiency
  static const int32_t KMinDataBuffsMgr = 2;
  static const int32_t KMinTaskBuffsMgr = 2;

  class LdaExecutorInit {
  private:

    BufferManager *extern_data_loader;
    BufferManager *task_buffer_mgr;
    BufferManager *data_buffer_mgr;
    BufferManager *extern_data_writer;

    zmq::context_t *zmq_ctx;
    DiskIOThread *diskio;
    RawTextParser *textparser;

    /* parameters that are set during initialization and cannot be changed afterwards*/

    /* parameters directly set by external arguments */
    int64_t membudget;
    int32_t buffsize;
    int32_t num_total_tasks; // total number of tasks

    std::string datadir; // path to directory to store internal data file
    std::vector<std::string> extern_data_paths;
    std::string internbase;

    int32_t num_topics;
    int32_t max_line_len;

    /* parameters that are computed duing init */
    int32_t num_total_buffs;
    int32_t num_task_buffs;
    int32_t num_data_buffs;
    int32_t num_tasks_per_buff;

    int32_t num_total_task_buffs; // this is the number of task buffers that's needed to hold all tasks
    /* END - parameters set during initialization*/

    /* parameters set during the initialization of data buffers and task buffers */
    int32_t num_my_init_data_buffs;
    Buffer **my_init_data_buffs;
    //int32_t data_buffs_idx;
    int32_t num_total_data_buffs; // number of buffers needed to hold all data

    int32_t num_my_task_buffs; // the set of task buffers that I'm currently executing
    Buffer **my_task_buffs;

    int32_t curr_task_st;
    int32_t curr_task_ed;

    // uses data_buff_acc, which could be in any state before invoking this function
    int init_task_buffers();
    int init_data_buffers();

    // can be called after initialization when all task buffers held are the first N buffers
    // or can be called when all buffers have been put back to task_buffer_mgr
    int output_task_buffers();
    // can be called after initialization when all
    int output_data_buffers();

  public:
   LdaExecutorInit();
   ~LdaExecutorInit();

   int init(int64_t _membudget, int32_t _buffsize, int32_t _num_words, int32_t _num_topics,
            std::string _datadir, std::vector<std::string> _extern_data_paths, std::string _internbase,
            int32_t _max_line_len);
   int run();
   int cleanup();
  };
}

#endif /* DISKSTREAM_LDA_EXECUTOR_HPP_ */

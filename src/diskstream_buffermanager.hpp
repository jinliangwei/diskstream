/*
 * diskstream_buffermanager.hpp
 *
 *  Created on: Jun 27, 2013
 *      Author: jinliang
 */

#ifndef __DISKSTREAM_BUFFERMANAGER_HPP__
#define __DISKSTREAM_BUFFERMANAGER_HPP__

#include <stdint.h>
#include <queue>
#include <string>
#include <map>

#include "diskstream_iothread.hpp"
#include "diskstream_buffer.hpp"
namespace diskstream {

  class BufferManager {
  private:
    EBufMgrState mgrstate;
    int64_t total_mem_size;
    int64_t free_mem_size;
    int32_t buff_size;
    DiskIOThread *diskio;
    int32_t diskio_cid;
    std::queue<Buffer *> free_buff_q;

    int32_t num_total_buffers; // total number of buffers managed by this mgr object, including
                                // free buffers (also unallocated memory), buffers given out,
                                // buffers in free and loaded queue, and buffers being processed by
                                // diskio thread.
    int32_t num_assigned_buffers; // number of buffers that's given to external uses
    int32_t num_io_buffers; // number of buffers that are being hold by iothread
    std::map<Buffer *, bool> buffers; // true if owned, false if not (released ...)

    // shared by BufMgrInit and BufMgrSequen
    std::string base;  // used for fullpath when used for write_extern

    // needed by BufMgrSequen
    int32_t load_iter;
    int32_t max_iter;
    int32_t putback_iter;
    int32_t putback_id;

    // needed by BufMgrSequen and BufMgrSequenExtern
    int32_t next_load_db_id; // the block id that is to be loaded next
    int32_t max_db_id;

    // needed by BufMgrSequenExtern
    int32_t extern_read_size;
    std::vector<std::string> seqextern_fullnames;
    std::vector<int64_t> fsizes;
    std::vector<int32_t> fmax_db_ids;

    int32_t curr_file_idx;
    int32_t access_iter; // block_id that's last accessed
    int32_t access_db_id; // file that's being accessed
    int32_t access_file_idx;

    int64_t extern_write_offset;

    int sequen_init_all_reads();
    int sequen_sched_read(Buffer *_buff);
    int sequen_sched_writeread(Buffer *_buff);

    int sequen_extern_init_all_reads();
    int sequen_extern_sched_read(Buffer *_buff);

    Buffer *get_buffer_init(); //also used for extern_write
    Buffer *get_buffer_sequen();
    Buffer *get_buffer_sequen_extern();

    int putback_init(Buffer *_buff, EBufMgrBufPolicy _policy);
    int putback_sequen(Buffer *_buff, EBufMgrBufPolicy _policy);
    int putback_sequen_extern(Buffer *_buff, EBufMgrBufPolicy _policy);
    int putback_write_extern(Buffer *_buff, EBufMgrBufPolicy _policy);

  public:
    BufferManager(int64_t _total_mem_size, int32_t _buff_size, DiskIOThread *_diskio);
    ~BufferManager();

    int init(std::string _base);
    int init_sequen(std::string _base, int32_t _db_st, int32_t _max_iter);
    int init_sequen_extern(std::vector<std::string> _fullnames);
    int init_write_extern(std::string _fullname);

    Buffer *get_buffer();
    //Buffer *get_buffer(int32_t &_size);
    int putback(Buffer *_buf, EBufMgrBufPolicy _policy);
    int stop();
    // if call on MgrSequen, all scheduled io read tasks are canceled, write tasks are still performed
    // if call on MgrInit, call diskio->cancle, then wait until all writes are done
    // if call on MgrSequenExtern,

    bool reached_end();

    // clients need to ensure the buffer being released has been assigned out
    //int release_buff(Buffer *_buff);
    int add_free_mem(int32_t _mem_size);
    int add_free_buffs(std::vector<Buffer *> _buffs);

    // get the size of free_buff_q
    int32_t get_num_free_buffs();
    int32_t get_free_mem_size();
    // return the buffers in free_buff_q and unallocated memory
    int release_all_free_buffs(std::vector<Buffer *> &_buffs, int32_t &_mem_size);
  };
}


#endif /* DISKSTREAM_BUFFERMANAGER_HPP_ */

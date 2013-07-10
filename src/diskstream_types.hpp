#ifndef __DISKSTREAM_TYPES_HPP__
#define __DISKSTREAM_TYPES_HPP__

#include <stdint.h>

namespace diskstream {
  typedef int32_t block_id_t;

  enum EBufMgrState {BufMgrInit, BufMgrSequen, BufMgrSequenExtern, BufMgrWriteExtern};
  enum EBufMgrBufPolicy {BufPolicyAbort, BufPolicyWriteBack};
  enum EDataCreateErr {DataCreateIgnore, DataCreateInsufficientMem};

  // BufMgrArbitrary is not yet supported.

  /*
   * Where could buffers be in each state?
   *
   * 1. BufMgrInit
   * 1) unallocated memory (free memory)
   * 2) free buffer queue # client may allocate more buffers to this manager, when it does so
   *                         the buffers are placed in free buffer queue
   * 3) assigned to clients
   * 4) being processed by diskio thread
   *
   * 2. BufMgrSequen
   * 1) being processed by diskio thread (cloud be reading or writing)
   * 2) assigned to clients
   * 3) in free buffer queue
   *
   * 3. BufMgrSequenExtern (almost the same as BufMgrSequen, but more freedom for file access)
   * 1) being processed by diskio thread (cloud be reading or writing)
   * 2) assigned to clients
   * 3) in free buffer queue
   * */

  /*
   * To be noticed about EBufMgrState
   *
   * BufMgrInit state
   *
   * In this state, the BufferManager object gives out free buffers for clients to initialize.
   * The buffers are backed by a set of internal files, therefore, the initialized buffers can be
   * put back. When BufPolicyWriteBack is specified, the buffer is written
   * to disk, where offset is the specified by the block id.
   *
   * main logic:
   *
   * get_buffer():
   *
   * 1) if free_mem_size >= buff_size -> allocate new buffer
   * 2) else if free_buff_q not empty() -> get from free_buff_q
   * 3) else if num_assigned_buffers < num_total_buffers -> keep getting response from diskio thread
   *    until the response is a BufPolicyWriteBack
   * 4) else return error (all buffers have been assigned to outside)
   * for 1), 2) and 3) do num_assigned_buffers + 1
   *
   * putback():
   *
   * if policy == BufPolicyAbort -> put to free_buff_q
   * else (policy == BufPolicyWriteBack-> call diskio.write())
   *         (bufmgr needs to set proper buffer state)
   *
   * BufMgrSequen state:
   *
   * In this state, the BufferManager iteratively loads data into buffers and give out loaded buffers
   * in the order of original data (same as buffer's datablock id). The maximum number of iterations
   * may be specified.
   *
   * When init_sequen() is invoked, all free buffers (unallocated memory are allocated), are scheduled
   * to read. When user requests buffers, BufferManager starts retrieving responses from diskio thread
   * until it gets a loaded buffer.
   *
   * BufMgrSequen ensures the order of the data buffer returned by ensuring the reading tasks are scheduled
   * in order.
   *
   * Note that the total buffer size might be larger than the data file size. In that case, the diskio
   * thread should return read size = 0. The buffer is then placed into free_buff_q.
   *
   * Client is suggested to check the number of free buffers after the first iteration so that free
   * buffers can be re-claimed
   *
   * main logic:
   *
   * get_buffer():
   *
   * It guarantees the buffers are given out in order. It also guarantees the same buffer will not
   * be given out unless it has been put back.
   *
   * To guarantee the same buffer won't be ginve out twice, it assumes the buffers are put back in
   * order. Therefore, it only needs to keep two variables for the last buffer that's put back.
   *
   * This could be considered as a circular buffer of buffers. Two variables denotes the position where
   * the last buffer loaded in and two variables denotes the position the last buffer put back.
   * One variable denotes the iteration (iter), and the other variable denotes the data block id.
   * The put back position is initialized to iteration 0 and last data block id.
   *
   * if num_assigned_buffers + free_buff_q.size() < num_total_buffers (same as num_io_buffers > 0)
   *    -> get a proper response (loaded) from diskio thread
   *       and return (if gets a BufPolicyWriteBack, the buffer is scheduled to read).
   * else -> error!
   *
   * putback():
   *
   * It requires that the buffers are put back in order. That is, the buffer of data block 2 must be
   * put back after that of data block 1 and before that of data block 3.
   *
   * if policy == BufPolicyAbort -> schedule the buffer to read the next data block
   * else (policy == BufPolicyWriteBack) -> schedule the buffer to be written
   *
   * BufMgrSequenExternal:
   *
   * In this state, all free buffers are scheduled to read the set of external data files from its
   * beginning. Note that the difference with BufMgrSequen is that 1) it is give a set of files; 2)
   * it reads from the beginning of the first file.
   *
   * Here, get_buffer() may return error if all buffers has been given out or it has read all files.
   *
   *
   * */

  /*
   * Currently allowed state transition:
   * BufMgrInit -> BufMgrSequen
   *
   * When a BufferManager is constructed, it is in BufMgrInit.
   *
   * */

}

#endif

#ifndef __DISKSTREAM_IOTHREAD_HPP__
#define __DISKSTREAM_IOTHREAD_HPP__

#include <zmq.hpp>
#include <syscall.h>
#include <fcntl.h>
#include <string>
#include <vector>

#include "diskstream_buffer.hpp"
#include "diskstream_types.hpp"
#include "diskstream_msgtypes.hpp"

namespace diskstream {

  /*
   * DiskIOThread needs to guarantee that the tasks scheduled by one sender are performed in the order
   * that they are scheduled. I could not yet find such specification for 0mq socket. The client of
   * DiskIOThread needs to check that and I will probably change it to something else.
   * */
  class DiskIOThread {
  private:
    // use router sock because there could be unlimited number of BufferManager
    // uses one DiskIOThread object
    zmq::context_t *zmq_ctx;
    zmq::socket_t sock;
    int32_t myid;
    int32_t client_cnt;
    int32_t max_clients;
    zmq::socket_t **client_sock; // (cid - 1) is index into the client_sock array, 0 if termination sock
    int status; //0 for normal, negative for error
    std::string inproc_endp;
    std::string datapath;
    pthread_t pthr;

    zmq::socket_t cancel_sock;
    zmq::socket_t cancel_push;

    bool *client_valid;

    /*
     * There are 2 id/indices:
     * 1) client id
     * 2) client_sock array index
     *
     * sock_idx = client_id - 1
     *
     * client_id is converted to zmq id when used to send message.
     *
     * client_id 0: myid
     * client_id 1: termination id
     * */

    static void *run_thread(void *_thr_info);

    static const int32_t KMAX_FSIZE = 1024*1024*1024; // 1 GB
    static int32_t cid_to_sock_idx(int32_t _cid);

    static int writen(int _fd, const uint8_t *_data, int32_t _size);
    static int readn(int _fd, uint8_t *_data, int32_t _size);
    static int reg_write(const uint8_t *buff, int32_t _size, const char *_filename,
                               int64_t offset);
    static int reg_read(uint8_t *buff, int32_t _size, const char *_filename,
                                   int64_t offset);
    static int dir_write(const uint8_t *_buff, int32_t _size, const char *_filename,
                                  int64_t offset);
    // return less than requested means EOF is reached
    static int32_t dir_read(uint8_t *_buff, int32_t _size, const char *_filename,
                            int64_t _offset);
    // DiskIOThread assumes the blocks written under the same basename are of the same size
    static int32_t get_file_idx(block_id_t _bid, int32_t _bsize, int64_t &_offset); // convert block id to file idx
    static std::string build_filename(std::string _datapath, std::string _basename, int32_t _fidx);

  public:
    DiskIOThread(zmq::context_t *_zmq_ctx, int32_t _max_clients,
                 const char *_inproc_endp, std::string _datadir);
    ~DiskIOThread();
    int init();
    // register_client() must happen after start()
    int32_t register_client(); // return the client id that's assigned to this client
    int start();
    int stop();
    int enable(int32_t _cid); // cancel disables the corresponding client to read from disk,
                                // call enable() to reenable reading
    int cancel(int32_t _cid);

    /*
     * sche_write(read)(external) requires the maximum size of data accessed is 1 GB.
     * Otherwise, int32_t is not large enough to return the size actually accessed
     * */

    // return 0 if the task is successfully scheduled
    int sche_write(int32_t _cid, const Buffer *_buff, const char *_basename);
    // return 0 if the task is successfully scheduled
    int sche_read(int32_t _cid, const Buffer *_buff, const char *_basename);

    int sche_write_read(int32_t _cid, const Buffer *_buff, const char *_basename, int32_t _rd_db_id);

    //for reading/writing external files
    // return 0 if the task is successfully scheduled
    int sche_write_extern(int32_t _cid, const Buffer *_buff, const char *_fullpath, int64_t _offset,
                          int32_t _size);
    // return 0 if the task is successfully scheduled
    int sche_read_extern(int32_t _cid, const Buffer *_buff, const char *_fullpath, int64_t _offset,
                         int32_t _size);
    int get_response(int32_t _cid, Buffer * &_buff, EMsgType &_type, int32_t &_suc);

    std::vector<int64_t> get_all_sizes(std::string _basename);
    static std::vector<int64_t> get_all_sizes(std::vector<std::string> _fnames);

  };
}
#endif

#ifndef __DISKSTREAM_IOTHREAD_HPP__
#define __DISKSTREAM_IOTHREAD_HPP__

#include <zmq.hpp>
#include <syscall.h>
#include <fcntl.h>
#include <string>

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

    static const uint32_t KMAX_FSIZE = 1024*1024*1024; // 1 GB
    static int32_t cid_to_sock_idx(int32_t _cid);

    /*
     * pwriten and preadn currently are not used
     * */
    static int pwriten(int _fd, const uint8_t *_data, uint32_t _size, uint32_t offset);
    static int preadn(int _fd, uint8_t *_data, uint32_t _size, uint32_t offset);

    static int writen(int _fd, const uint8_t *_data, uint32_t _size);
    static int readn(int _fd, uint8_t *_data, uint32_t _size);
    static int dir_write(const uint8_t *_buff, uint32_t _size, const char *_filename,
                         uint32_t offset);

    // return less than requested means EOF is reached
    static int32_t dir_read(uint8_t *_buff, uint32_t _size, const char *_filename,
                     uint32_t _offset);
    // DiskIOThread assumes the blocks written under the same basename are of the same size
    static int32_t get_file_idx(block_id_t _bid, uint32_t _bsize, uint32_t &_offset); // convert block id to file idx
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

    /*
     * sche_write(read)(external) requires the maximum size of data accessed is 1 GB.
     * Otherwise, int32_t is not large enough to return the size actually accessed
     * */

    // return 0 if the task is successfully scheduled
    int sche_write(int32_t _cid, const Buffer *_buff, const char *_basename);
    // return 0 if the task is successfully scheduled
    int sche_read(int32_t _cid, const Buffer *_buff, const char *_basename);

    //for reading/writing external files
    // return 0 if the task is successfully scheduled
    int sche_write_extern(int32_t _cid, const Buffer *_buff, const char *_fullpath, uint32_t _offset,
                          uint32_t _size);
    // return 0 if the task is successfully scheduled
    int sche_read_extern(int32_t _cid, const Buffer *_buff, const char *_fullpath, uint32_t _offset,
                         uint32_t _size);
    int get_response(int32_t _cid, Buffer * &_buff, EMsgType &_type, int32_t &_suc);

  };
}
#endif

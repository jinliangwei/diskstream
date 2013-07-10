
// setting this macro to 64 allows I/Os on large files (> 2 GB) on 32-bit systems
// this is preferred than using O_LARGEFILE flag
// I/Os on large files are naturally permitted on 64-bit systems
// reference: http://man7.org/linux/man-pages/man7/feature_test_macros.7.html
#define _FILE_OFFSET_BITS 64

#include <zmq.hpp>
#include <pthread.h>
#include <new>
#include <sstream>
#include <assert.h>
#include <string.h>
#include <dirent.h>
#include <sys/types.h>
#include <map>
#include <sys/stat.h>
#include <unistd.h>

#include "diskstream_iothread.hpp"
#include "diskstream_types.hpp"
#include "util/diskstream_zmq_util.hpp"
#include "util/diskstream_util.hpp"


// TODO: should map each cid to a base file name

namespace diskstream{
  const char *diskio_cancel_endp = "inproc://disio_cancel_endp";

  // zmq_context is thread-safe
  DiskIOThread::DiskIOThread(zmq::context_t *_zmq_ctx, int32_t _max_clients,
                             const char *_inproc_endp, std::string _datapath):
      zmq_ctx(_zmq_ctx),
      sock(*_zmq_ctx, ZMQ_ROUTER),
      myid(0),
      client_cnt(1), // leave 1 for termination sock
      max_clients(_max_clients + 1), // the first client (cid 1) is used to send shutdonw signal
      client_sock(NULL),
      status(-1),
      inproc_endp(_inproc_endp),
      datapath(_datapath),
      pthr(),
      cancel_sock(*_zmq_ctx, ZMQ_PULL),
      cancel_push(*_zmq_ctx, ZMQ_PUSH),
      client_valid(NULL){}

  DiskIOThread::~DiskIOThread(){
    if(client_sock != NULL){
      int i;
      for(i = 0; i < max_clients; ++i){
        if(client_sock[i] != NULL){
          delete client_sock[i];
        }
      }
      delete[] client_sock;
    }
  }

  int DiskIOThread::init(){
    int sock_mandatory = 1;
    try{
      int32_t zmqid = cid_to_zmq_rid(myid);
      sock.setsockopt(ZMQ_IDENTITY, &zmqid, sizeof(int32_t));
      sock.setsockopt(ZMQ_ROUTER_MANDATORY, &sock_mandatory, sizeof(int));
      sock.bind(inproc_endp.c_str());
    }catch(zmq::error_t &e){
      std::cout << e.what() << std::endl;
      return -1;
    }
    try{
      client_sock = new zmq::socket_t *[max_clients];
      memset(client_sock, 0, sizeof(zmq::socket_t *)*max_clients);
      client_valid = new bool[max_clients];
      memset(client_valid, 0, sizeof(bool)*max_clients);
    }catch(std::bad_alloc &e){
      return -1;
    }

    try{
      cancel_sock.bind(diskio_cancel_endp);
      cancel_push.connect(diskio_cancel_endp);
    }catch(...){
      return -1;
    }

    try{
      client_sock[0] = new zmq::socket_t(*zmq_ctx, ZMQ_ROUTER);
      int32_t zmqid = cid_to_zmq_rid(1); // 1 is termination socket id
      client_sock[0]->setsockopt(ZMQ_IDENTITY, &zmqid, sizeof(int32_t));
      client_sock[0]->setsockopt(ZMQ_ROUTER_MANDATORY, &sock_mandatory, sizeof(int));
      client_sock[0]->connect(inproc_endp.c_str());
    }catch(...){
      return -1;
    }
    status = 0;
    return 0;
  }

  //TODO: find the first idx making valid_sock[idx] == false and use that idx.
  int32_t DiskIOThread::register_client(){
    if(status < 0) return -1;
    if(client_cnt >= max_clients) return -1;
    int32_t idx = client_cnt;
    int32_t cid = ++client_cnt;
    int sock_mandatory = 1;
    try{
      client_sock[idx] = new zmq::socket_t(*zmq_ctx, ZMQ_ROUTER);
      int32_t zmqid = cid_to_zmq_rid(cid); // 1 is termination socket id
      client_sock[idx]->setsockopt(ZMQ_IDENTITY, &zmqid, sizeof(int32_t));
      client_sock[idx]->setsockopt(ZMQ_ROUTER_MANDATORY, &sock_mandatory, sizeof(int));
      //int linger = 1;
      //client_sock[idx]->setsockopt(ZMQ_LINGER, &linger, sizeof(int)); // disgard pending send when socket is closed
      client_sock[idx]->connect(inproc_endp.c_str());
    }catch(...){
      return -1;
    }

    int32_t zid = cid_to_zmq_rid(myid);
    int32_t ret;
    EMsgType type = DiskIOConn;
    //std::cout << "send msg from cid = " << cid << " to zid = " << zid << std::endl;
    do{
      ret = send_msg(*client_sock[idx], zid, (uint8_t *) &type, sizeof(EMsgType), 0);
    }while(ret < 0);
    client_valid[idx] = true;
    return cid;
  }

  // TODO: read/write external should use buffer->valid_db_size instead of passing in additional
  // parameters
  void *DiskIOThread::run_thread(void *_thr_info){
    DiskIOThread *diskio = (DiskIOThread *) _thr_info;

    int ret = zmq_socket_monitor(diskio->sock, "inproc://monitor.router_sock", ZMQ_EVENT_CONNECTED | ZMQ_EVENT_ACCEPTED | ZMQ_EVENT_DISCONNECTED);
    if(ret < 0){
      diskio->status = -1;
      return NULL;
    }

    zmq::socket_t monitor_sock(*diskio->zmq_ctx, ZMQ_PAIR);
    try{
      monitor_sock.connect("inproc://monitor.router_sock");
    }catch(zmq::error_t &e){
      std::cout << "monitor socket create failed" << std::endl;
      return NULL;
    }

    zmq::pollitem_t pollitems[3];
    pollitems[0].socket = diskio->sock;
    pollitems[0].events = ZMQ_POLLIN;
    pollitems[1].socket = monitor_sock;
    pollitems[1].events = ZMQ_POLLIN;
    pollitems[2].socket = diskio->cancel_sock;
    pollitems[2].events = ZMQ_POLLIN;

    //std::cout << "diskio thread is running" << std::endl;

    while(true){
      try {
        zmq::poll(pollitems, 3);
      }catch(...){
        std::cout << "error from poll!" << std::endl;
        return NULL;
      }
      //std::cout << "diskio thread get message!" << std::endl;

      if(pollitems[2].revents){
        int32_t cid;
        boost::shared_array<uint8_t> data;
        int32_t len = recv_msg(diskio->cancel_sock, cid, data);
        if(len < 0){
          diskio->status = -1;
          return NULL;
        }

        EMsgType type = *((EMsgType *) data.get());
        int32_t extern_cid, sock_idx;

        switch(type){
        case DiskIOCancel:
          extern_cid = zmq_rid_to_cid(cid);
          sock_idx = cid_to_sock_idx(extern_cid);
          diskio->client_valid[sock_idx] = false;
          std::cout << "canceled " << extern_cid << std::endl;
          break;
        case DiskIOEnable:
          extern_cid = zmq_rid_to_cid(cid);
          sock_idx = cid_to_sock_idx(extern_cid);
          diskio->client_valid[sock_idx] = true;
          std::cout << "enabled " << extern_cid << std::endl;
          break;
        default:
          assert(0);
          diskio->status = -1;
          return NULL;
        }
        continue;
      }

      if(pollitems[0].revents){
        int32_t cid;
        boost::shared_array<uint8_t> data;
        int32_t len = recv_msg(diskio->sock, cid, data);
        if(len < 0){
          assert(0);
          diskio->status = -1;
          return NULL;
        }
        //std::cout << "received from cid = " << std::hex << cid << std::dec << std::endl;
        EMsgType type = *((EMsgType *) data.get());
        Buffer *buf;
        std::string base; // for DiskIOExternal, base is actually fullpath
        int32_t extern_cid;
        int32_t sock_idx;
        if(type == DiskIORead || type == DiskIOWrite || type == DiskIOReadExternal
            || type == DiskIOWriteExternal || type == DiskIOWriteRead){
          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          buf = *((Buffer **) data.get());
          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          base = std::string((char *) data.get());
        }

        block_id_t bid;
        int32_t extern_size;
        int32_t dbsize;
        int64_t offset;
        int32_t fidx;
        int32_t suc;
        std::string fname;
        int32_t rsize;
        int32_t wsuc;
        int32_t rd_db_id;
        EMsgType re_type;
        switch(type){
        case DiskIORead:
          bid = buf->get_block_id();

          dbsize = buf->get_db_size();
          fidx = get_file_idx(bid, dbsize, offset);
          fname = build_filename(diskio->datapath, base, fidx);

          extern_cid = zmq_rid_to_cid(cid);
          sock_idx = cid_to_sock_idx(extern_cid);
          if(!diskio->client_valid[sock_idx]){
            rsize = 0;
            re_type = DiskIOReadNotDone;
          }else{
            // read data from specified file, with
            rsize = dir_read(buf->get_db_ptr(), dbsize, fname.c_str(), offset);
            re_type = DiskIOReadDone;
          }

          // finished reading send back message
          suc = send_msg(diskio->sock, cid, (uint8_t *) &re_type, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          send_msg(diskio->sock, (uint8_t *) &buf, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          suc = send_msg(diskio->sock, (uint8_t *) &rsize, sizeof(int32_t), 0);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          break;

        case DiskIOWrite:
          //std::cout << "received request to write data" << std::endl;
          //std::cout << "buf = " << (void *) buf << std::endl;
          //std::cout << "basename = " << base.c_str() << std::endl;

          bid = buf->get_block_id();

          dbsize = buf->get_db_size();
          fidx = get_file_idx(bid, dbsize, offset);
          fname = build_filename(diskio->datapath, base, fidx);

          // read data from specified file, with
          wsuc = dir_write(buf->get_db_ptr(), dbsize, fname.c_str(), offset);
          re_type = DiskIOWriteDone;

          // finished reading send back message
          suc = send_msg(diskio->sock, cid, (uint8_t *) &re_type, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          send_msg(diskio->sock, (uint8_t *) &buf, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          suc = send_msg(diskio->sock, (uint8_t *) &wsuc, sizeof(int32_t), 0);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          break;
        case DiskIOWriteRead:
          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          rd_db_id = *((int32_t *) data.get());

          bid = buf->get_block_id();
          dbsize = buf->get_db_size();
          fidx = get_file_idx(bid, dbsize, offset);
          fname = build_filename(diskio->datapath, base, fidx);

          // write data to
          wsuc = dir_write(buf->get_db_ptr(), dbsize, fname.c_str(), offset);

          int32_t resp_val;

          extern_cid = zmq_rid_to_cid(cid);
          sock_idx = cid_to_sock_idx(extern_cid);
          if(!diskio->client_valid[sock_idx]){
            resp_val = 0;
            re_type = DiskIOWriteReadWriteDone;
          }else{
            if(wsuc == 0){
              fidx = get_file_idx(rd_db_id, dbsize, offset);
              fname = build_filename(diskio->datapath, base, fidx);
              rsize = dir_read(buf->get_db_ptr(), dbsize, fname.c_str(), offset);
            }
            if(wsuc < 0 || rsize < 0){
              resp_val = -1;
            }else{
              resp_val = rsize;
            }
            re_type = DiskIOWriteReadDone;
          }
          // finished reading send back message
          suc = send_msg(diskio->sock, cid, (uint8_t *) &re_type, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          send_msg(diskio->sock, (uint8_t *) &buf, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            diskio->status = -1;
            return NULL;
          }
          suc = send_msg(diskio->sock, (uint8_t *) &resp_val, sizeof(int32_t), 0);
          if(suc < 0){
            diskio->status = -1;
            return NULL;
          }
          break;
        case DiskIOReadExternal:
          //std::cout << "buf = " << (void *) buf << std::endl;
          //std::cout << "fullpath = " << base.c_str() << std::endl;

          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          offset = *((int64_t *) data.get());

          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          extern_size = *((int32_t *) data.get());
          //std::cout << "size to read = " << extern_size << std::endl;

          extern_cid = zmq_rid_to_cid(cid);
          sock_idx = cid_to_sock_idx(extern_cid);
          if(!diskio->client_valid[sock_idx]){
            rsize = 0;
            re_type = DiskIOReadExternalNotDone;
          }else{
            //std::cout << "received request to read external data" << std::endl;
            dbsize = buf->get_db_size();
            if(extern_size > dbsize){
              assert(0);
              diskio->status = -1;
              return NULL;
            }
            fname = std::string(base);
            // read data from specified file, with
            rsize = dir_read(buf->get_db_ptr(), extern_size, fname.c_str(), offset);
            re_type = DiskIOReadExternalDone;
          }
          buf->set_valid_db_size(rsize);

          // finished reading send back message
          suc = send_msg(diskio->sock, cid, (uint8_t *) &re_type, sizeof(EMsgType),
                         ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }

          send_msg(diskio->sock, (uint8_t *) &buf, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }

          suc = send_msg(diskio->sock, (uint8_t *) &rsize, sizeof(int32_t), 0);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          break;

        case DiskIOWriteExternal:
          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          offset = *((int64_t *) data.get());

          len = recv_msg(diskio->sock, data);
          if(len < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }

          // if has been canceled, don't send response
          extern_cid = zmq_rid_to_cid(cid);
          sock_idx = cid_to_sock_idx(extern_cid);
          if(diskio->client_sock[sock_idx] == NULL) continue;

          extern_size = *((int32_t *) data.get());

          dbsize = buf->get_db_size();
          //std::cout << "extern_size = " << extern_size << " dbsize = " << dbsize << std::endl;
          assert(extern_size <= dbsize);

          fname = std::string(base);

          // read data from specified file, with
          rsize = reg_write(buf->get_db_ptr(), extern_size, fname.c_str(), offset);
          re_type = DiskIOWriteExternalDone;

          // finished reading send back message
          suc = send_msg(diskio->sock, cid, (uint8_t *) &re_type, sizeof(Buffer *),
              ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          send_msg(diskio->sock, (uint8_t *) &buf, sizeof(Buffer *), ZMQ_SNDMORE);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          suc = send_msg(diskio->sock, (uint8_t *) &rsize, sizeof(int32_t), 0);
          if(suc < 0){
            assert(0);
            diskio->status = -1;
            return NULL;
          }
          break;
        case DiskIOShutDown:
          std::cout << "diskio thread received shutdown message" << std::endl;
          return NULL;
          break;
        case DiskIOConn:
          break;
        default:
          assert(0);
          diskio->status = -1;
          return NULL;
        }
        continue;
      }

      if(pollitems[1].revents){
        zmq_event_t *event;
        boost::shared_array<uint8_t> data;
        int len;
        len = recv_msg(monitor_sock, data);

        if (len < 0){
          assert(0);
          diskio->status = -11;
          return NULL;
        }
        assert(len == sizeof(zmq_event_t));
        event = (zmq_event_t *) data.get();

        switch (event->event){
        case ZMQ_EVENT_CONNECTED:
          std::cout << "established connection." << std::endl;
          break;
        case ZMQ_EVENT_ACCEPTED:
          std::cout << "connection accepted" << std::endl;
          break;
        case ZMQ_EVENT_DISCONNECTED:
          std::cout << "client disconnected" << std::endl;
          break;
        default:
          std::cout << "unexpected event" << std::endl;
          return NULL;
        }
        continue;
      }
    }
    return NULL;
  }

  int DiskIOThread::start(){
    if(status < 0) return -1;
    int ret = pthread_create(&pthr, NULL, run_thread, this);
    if(ret != 0) return -1;
    return 0;
  }

  int DiskIOThread::stop(){
    int32_t zid = cid_to_zmq_rid(myid);
    EMsgType type = DiskIOShutDown;
    int suc = send_msg(*client_sock[0], zid, (uint8_t *) &type, sizeof(EMsgType), 0);
    if(suc < 0) return suc;
    std::cout << "has sent message to shut down the io thread" << std::endl;
    suc = pthread_join(pthr, NULL);
    std::cout << "diskio thread joined" << std::endl;
    if(suc < 0) return suc;
    else return 0;
  }

  int DiskIOThread::sche_write(int32_t _cid, const Buffer *_buff, const char *_basename){
    if(status < 0) return -1;

    EMsgType type = DiskIOWrite;
    int32_t zid = cid_to_zmq_rid(myid);
    int32_t sock_idx = cid_to_sock_idx(_cid);

    if(client_sock[sock_idx] == NULL) return -1;

    int suc = send_msg(*client_sock[sock_idx], zid, (uint8_t *) &type, sizeof(EMsgType), ZMQ_SNDMORE);
    if(suc < 0) return -1;
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_buff, sizeof(Buffer *), ZMQ_SNDMORE);
    if(suc < 0) return -1;
    // data sent must include \0
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) _basename, strlen(_basename) + 1, 0);
    if(suc < 0) return -1;
    return 0;
  }

  int DiskIOThread::sche_read(int32_t _cid, const Buffer *_buff, const char *_basename){
    if(status < 0) return -1;

    EMsgType type = DiskIORead;
    int32_t zid = cid_to_zmq_rid(myid);
    int32_t sock_idx = cid_to_sock_idx(_cid);

    if(client_sock[sock_idx] == NULL) return -1;

    int suc = send_msg(*client_sock[sock_idx], zid, (uint8_t *) &type, sizeof(EMsgType), ZMQ_SNDMORE);
    if(suc < 0) return -1;
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_buff, sizeof(Buffer *), ZMQ_SNDMORE);
    if(suc < 0) return -1;
    // data sent must include \0
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) _basename, strlen(_basename) + 1, 0);
    if(suc < 0) return -1;
    return 0;
  }

  int DiskIOThread::sche_write_read(int32_t _cid, const Buffer *_buff, const char *_basename,
                                    int32_t _rd_db_id){
    if(status < 0) return -1;

    EMsgType type = DiskIORead;
    int32_t zid = cid_to_zmq_rid(myid);
    int32_t sock_idx = cid_to_sock_idx(_cid);

    if(client_sock[sock_idx] == NULL) return -1;

    int suc = send_msg(*client_sock[sock_idx], zid, (uint8_t *) &type, sizeof(EMsgType), ZMQ_SNDMORE);
    if(suc < 0) return -1;
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_buff, sizeof(Buffer *), ZMQ_SNDMORE);
    if(suc < 0) return -1;
    // data sent must include \0
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) _basename, strlen(_basename) + 1, ZMQ_SNDMORE);
    if(suc < 0) return -1;
    suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_rd_db_id,  sizeof(int32_t), 0);
    if(suc < 0) return -1;
    return 0;

  }

  int DiskIOThread::sche_write_extern(int32_t _cid, const Buffer *_buff, const char *_fullpath,
                                      int64_t _offset, int32_t _size){
      assert(status == 0);
      if(status < 0) return -1;

      EMsgType type = DiskIOWriteExternal;
      int32_t zid = cid_to_zmq_rid(myid);
      int32_t sock_idx = cid_to_sock_idx(_cid);

      if(client_sock[sock_idx] == NULL) return -1;

      int suc = send_msg(*client_sock[sock_idx], zid, (uint8_t *) &type, sizeof(EMsgType), ZMQ_SNDMORE);
      if(suc < 0) return -1;
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_buff, sizeof(Buffer *), ZMQ_SNDMORE);
      if(suc < 0) return -1;
      // data sent must include \0
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) _fullpath, strlen(_fullpath) + 1, ZMQ_SNDMORE);
      if(suc < 0) return -1;
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_offset, sizeof(int64_t), ZMQ_SNDMORE);
      if(suc < 0) return -1;
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_size, sizeof(int32_t), 0);
      if(suc < 0) return -1;
      return 0;
    }

    int DiskIOThread::sche_read_extern(int32_t _cid, const Buffer *_buff, const char *_fullpath,
                                       int64_t _offset, int32_t _size){
      if(status < 0) return -1;
      EMsgType type = DiskIOReadExternal;
      int32_t zid = cid_to_zmq_rid(myid);
      int32_t sock_idx = cid_to_sock_idx(_cid);

      if(client_sock[sock_idx] == NULL) return -1;

      int suc = send_msg(*client_sock[sock_idx], zid, (uint8_t *) &type, sizeof(EMsgType), ZMQ_SNDMORE);
      if(suc < 0) return -1;
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_buff, sizeof(Buffer *), ZMQ_SNDMORE);
      if(suc < 0) return -1;
      // data sent must include \0
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) _fullpath, strlen(_fullpath) + 1, ZMQ_SNDMORE);
      if(suc < 0) return -1;
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_offset, sizeof(int64_t), ZMQ_SNDMORE);
      if(suc < 0) return -1;
      suc = send_msg(*client_sock[sock_idx], (uint8_t *) &_size, sizeof(int32_t), 0);
      if(suc < 0) return -1;
      return 0;
    }

  int DiskIOThread::get_response(int32_t _cid, Buffer * &_buff, EMsgType &_type, int32_t &_suc){
    if(status < 0) return -1;
    int32_t sock_idx = cid_to_sock_idx(_cid);
    int32_t srcid;

    boost::shared_array<uint8_t> data;
    //std::cout << "get_response called from cid = " << std::hex << _cid << std::dec << std::endl;
    int32_t suc = recv_msg(*client_sock[sock_idx], srcid, data);
    if(suc <= 0) return -1;
    _type = *((EMsgType *) data.get());
    suc = recv_msg(*client_sock[sock_idx], data);
    if(suc <= 0) return -1;
    _buff = *((Buffer **) data.get());
    suc = recv_msg(*client_sock[sock_idx], data);
    if(suc <= 0) return -1;
    _suc = *((int32_t *) data.get());
    return 0;
  }

  int DiskIOThread::cancel(int32_t _cid){
    int32_t zid = cid_to_zmq_rid(_cid);
    int32_t sock_idx = cid_to_sock_idx(_cid);
    if(client_sock[sock_idx] == NULL) return -1;
    EMsgType type = DiskIOCancel;
    int suc = send_msg(cancel_push, zid, (uint8_t *) &type, sizeof(EMsgType), 0);
    if(suc < 0) return -1;
    return 0;
  }

  int DiskIOThread::enable(int32_t _cid){
      int32_t zid = cid_to_zmq_rid(_cid);
      int32_t sock_idx = cid_to_sock_idx(_cid);
      if(client_sock[sock_idx] == NULL) return -1;
      EMsgType type = DiskIOEnable;
      int suc = send_msg(cancel_push, zid, (uint8_t *) &type, sizeof(EMsgType), 0);
      if(suc < 0) return -1;
      return 0;
    }

  int32_t DiskIOThread::cid_to_sock_idx(int32_t _cid){
    return (_cid - 1);
  }

  int DiskIOThread::writen(int _fd, const uint8_t *_data, int32_t _size){
    int32_t i = 0;
    while(i < _size){
      int32_t written = write(_fd, _data + i, (_size - i));
        if(written < 0) return written;
        i += (int32_t) written;
    }
    return 0;
  }

  int32_t DiskIOThread::readn(int _fd, uint8_t *_data, int32_t _size){
    int32_t readtotal = 0;
    do{
      int32_t readbytes = read(_fd, _data, _size);
      if(readbytes == 0){
        //std::cout << "reached the end of the file!" << std::endl;
        //std::cout << "readtotal = " << readtotal << std::endl;
        return readtotal;
      }else if(readbytes < 0){
        //display_errno();
        return readbytes;
      }else{
        readtotal += readbytes;
      }
    }while(readtotal < _size);
    return readtotal;
  }

  // max file size is 2 GB
  int DiskIOThread::dir_write(const uint8_t *_buff, int32_t _size, const char *_filename,
                              int64_t offset){
    int fd = open(_filename, O_CREAT | O_WRONLY | O_DIRECT | O_FSYNC, S_IRWXU);
    if(fd < 0) return fd;
    lseek(fd, offset, SEEK_SET);
    int suc = writen(fd, _buff, _size);
    close(fd);
    return suc;
  }

  int DiskIOThread::reg_read(uint8_t *_buff, int32_t _size, const char *_filename,
                              int64_t offset){
    int fd = open(_filename, O_RDONLY);
    if(fd < 0) return fd;
    lseek(fd, offset, SEEK_SET);
    int suc = readn(fd, _buff, _size);
    close(fd);
    return suc;
  }

  int DiskIOThread::reg_write(const uint8_t *_buff, int32_t _size, const char *_filename,
                              int64_t offset){
    int fd = open(_filename, O_CREAT | O_WRONLY, S_IRWXU);
    if(fd < 0) return fd;
    lseek(fd, offset, SEEK_SET);
    int suc = writen(fd, _buff, _size);
    close(fd);
    return suc;
  }

  int32_t DiskIOThread::dir_read(uint8_t *_buff, int32_t _size, const char *_filename,
                                 int64_t offset){
    int fd = open(_filename, O_RDONLY | O_DIRECT);
    if(fd < 0) return fd;
    //std::cout << "open file " << _filename << " OK " << std::endl;
    lseek(fd, offset, SEEK_SET);
    int suc = readn(fd, _buff, _size);
    close(fd);
    //std::cout << "readn returns suc = " << suc << std::endl;
    return suc;
  }
  int32_t DiskIOThread::get_file_idx(block_id_t bid, int32_t bsize, int64_t &offset){
    int32_t blocks_per_file = KMAX_FSIZE/bsize;
    int32_t fidx = bid/blocks_per_file;
    offset = bid%blocks_per_file;
    return fidx;
  }

  std::string DiskIOThread::build_filename(std::string _datapath, std::string _basename, int32_t _fidx){
    std::stringstream ss;
    ss << _datapath << "/" << _basename << "." << _fidx << ".diskio";
    return ss.str();
  }

 std::vector<int32_t> DiskIOThread::get_all_sizes(std::string _basename){
    DIR *dir;
    struct dirent *fent;
    dir = opendir(datapath.c_str());
    if(dir == NULL) return std::vector<int32_t>();
    std::cout << "open dir worked" << std::endl;
    int num_files = 0;
    while((fent = readdir(dir)) != NULL){
      //int len = strlen(fent->d_name);
      std::cout << "get file: " << fent->d_name << std::endl;
      int cmp = strncmp(fent->d_name, _basename.c_str(), _basename.size());
      if(cmp == 0) ++num_files;

    }
    closedir(dir);
    std::cout << "num_files = " << num_files << std::endl;
    std::vector<int32_t> fsize(num_files);
    int i;
    for(i = 0; i < num_files; ++i){
      struct stat fstat;
      int fstat_suc = stat(build_filename(datapath, _basename, i).c_str(), &fstat);
      if(fstat_suc < 0) return std::vector<int32_t>();
      int32_t fs = fstat.st_size;
      fsize[i] = fs;
    }
    return fsize;
  }

 std::vector<int32_t> DiskIOThread::get_all_sizes(std::vector<std::string> _fnames){
   int32_t num_files = _fnames.size();
   std::vector<int32_t> fsize(num_files);
   int i;
   for(i = 0; i < num_files; ++i){
     struct stat fstat;
     int fstat_suc = stat(_fnames[i].c_str(), &fstat);
     if(fstat_suc < 0) return std::vector<int32_t>();
     int32_t fs = fstat.st_size;
     fsize[i] = fs;
   }
   return fsize;
 }
}

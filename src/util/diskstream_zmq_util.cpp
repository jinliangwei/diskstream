
#include "diskstream_zmq_util.hpp"

namespace diskstream {

  int32_t cid_to_zmq_rid(int32_t cid){
    return (cid << 4 | 0x1);
  }

  int32_t zmq_rid_to_cid(int32_t zid){
    return (zid >> 4);
  }

  /*
   * return number of bytes received, negative if error, 0 for received nothing, which should be treated as error
   */
  int32_t recv_msg(zmq::socket_t &sock, boost::shared_array<uint8_t> &data){
    zmq::message_t msgt;
    int nbytes;
    try{
      nbytes = sock.recv(&msgt);
    }catch(zmq::error_t &e){
      return -1;
    }

    if(nbytes == 0) return 0;

    size_t len = msgt.size();    
    uint8_t *dataptr;
    try{
      dataptr = new uint8_t[len];
    }catch(std::bad_alloc &e){
      return -1;
    }
    
    memcpy(dataptr, msgt.data(), len);
    data.reset(dataptr);
    return len;
  }

  /*
   * return number of bytes received, negative if error
   */
  int32_t recv_msg(zmq::socket_t &sock, int32_t &cid, boost::shared_array<uint8_t> &data){
    zmq::message_t msgt;
    try{
      sock.recv(&msgt);
    }catch(zmq::error_t &e){
      return -1;
    }

    size_t len = msgt.size();
    if(len != sizeof(int32_t)) return -1;

    cid = *((int32_t *) msgt.data());

    return recv_msg(sock, data);
  }

  /*
   * return number of bytes sent, negative if error
   */
  int32_t send_msg(zmq::socket_t &sock, uint8_t *data, size_t len, int flag){
    int nbytes;
    try{
      nbytes = sock.send(data, len, flag);
    }catch(zmq::error_t &e){
      std::cout << "error is " << zmq_strerror(e.num()) << std::endl;
      return -1;
    }
    return nbytes;
  }

  int32_t send_msg(zmq::socket_t &sock, int32_t cid, uint8_t *data, size_t len, int flag){
  
    int nbytes;
    try{
      nbytes = send_msg(sock, (uint8_t *) &cid, sizeof(int32_t), flag | ZMQ_SNDMORE);
    }catch(zmq::error_t &e){
      std::cout << "error is " << zmq_strerror(e.num()) << std::endl;
      return -1;
    }

    if(nbytes < 0) return -1;

    if(nbytes != sizeof(int32_t)){
      std::cout << "send cid bytes num does not match, nbytes = " << nbytes << std::endl;
      return -1;
    }
    return send_msg(sock, data, len, flag);
  }

  // 0 for received nothing
  int32_t recv_msg_async(zmq::socket_t &sock, boost::shared_array<uint8_t> &data){
    zmq::message_t msgt;
    int nbytes;
    try{
      nbytes = sock.recv(&msgt, ZMQ_DONTWAIT);
    }catch(zmq::error_t &e){
      return -1;
    }

    if(nbytes == 0){
      if(zmq_errno() == EAGAIN)
        return 0;
      else
        return -1;
    }

    size_t len = msgt.size();
    uint8_t *dataptr;
    try{
      dataptr = new uint8_t[len];
    }catch(std::bad_alloc &e){
      return -1;
    }
    memcpy(dataptr, msgt.data(), len);
    data.reset(dataptr);
    return len;
  }

  // 0 for received nothing
  int32_t recv_msg_async(zmq::socket_t &sock, int32_t &cid, boost::shared_array<uint8_t> &data){
    zmq::message_t msgt;
    int nbytes;
    try{
      nbytes = sock.recv(&msgt, ZMQ_DONTWAIT);
    }catch(zmq::error_t &e){
      return -1;
    }
    if(nbytes == 0){
      if(zmq_errno() == EAGAIN)
        return 0;
      else
        return -1;
    }
    size_t len = msgt.size();
    if(len != sizeof(int32_t)) return -1;
    cid = *((int32_t *) msgt.data());
    return recv_msg(sock, data); //it does not matter to use recv_msg_async or recv_msg
  }
}

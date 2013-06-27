#ifndef __DISKSTREAM_ZMQ_UTIL_HPP__
#define __DISKSTREAM_ZMQ_UTIL_HPP__

#include <zmq.hpp>
#include <assert.h>
#include <boost/shared_array.hpp>
#include "../diskstream_types.hpp"


namespace diskstream {

  int32_t cid_to_zmq_rid(int32_t cid);

  int32_t zmq_rid_to_cid(int32_t zid);

  /*
   * return number of bytes received, negative if error
   */
  int32_t recv_msg_async(zmq::socket_t &sock, boost::shared_array<uint8_t> &data);

  /*
   * return number of bytes received, negative if error
   */
  int32_t recv_msg_async(zmq::socket_t &sock, int32_t &cid, boost::shared_array<uint8_t> &data);

  /*
   * return number of bytes received, negative if error
   */
  int32_t recv_msg(zmq::socket_t &sock, boost::shared_array<uint8_t> &data);

  /*
   * return number of bytes received, negative if error
   */
  int32_t recv_msg(zmq::socket_t &sock, int32_t &cid, boost::shared_array<uint8_t> &data);


  /*
   * return number of bytes sent, negative if error
   */
  int32_t send_msg(zmq::socket_t &sock, uint8_t *data, size_t len, int flag);

  int32_t send_msg(zmq::socket_t &sock, int32_t cid, uint8_t *data, size_t len, int flag);
}
#endif


#include <string.h>
#include <new>
#include <iostream>

#include "diskstream_types.hpp"
#include "diskstream_buffer.hpp"


namespace diskstream {

  uint8_t *align_ptr(uint8_t *original, int32_t alignment){
#ifdef __LP64__
    int64_t ptr = (int64_t) original;
    int64_t align = alignment - 1;
    ptr = ptr & (~align);
#else
    int32_t ptr = (int32_t) original;
    int32_t align = alignment - 1;
    ptr = ptr & (~align);
#endif
    return (uint8_t *) ptr;
  }

  DataBlock::DataBlock():
    data_offset(sizeof(DataBlock)),
    last_data_offset(data_offset),
    dsize(0),
    num_data(0){}

  int DataBlock::append_data_record(uint8_t *_st, int32_t _size){
    last_data_offset = (_st - (uint8_t *) this);
    dsize += _size;
    ++num_data;
    return 0;
  }

  int32_t DataBlock::get_data_capacity(int32_t _db_size){
    return _db_size - sizeof(DataBlock);
  }


  Buffer::Buffer(int32_t _bsize, block_id_t _dbid):
    bsize(_bsize),
    dbid(_dbid),
    valid_db_size(0){
    uint8_t *db_ptr = ((uint8_t *) this) + sizeof(Buffer);
    db_ptr = align_ptr(db_ptr + 512, 512);
    db = new (db_ptr) DataBlock();
    //TODO: after testing, replace with get_db_size function
    // must ensure that all buffers have the same size of meaningful data
    int num = sizeof(Buffer)/512 + 1; // number of 512B-sized blocks that Buffer object occupys.
    datasize = bsize - 512*num - 512;
    db_end = db_ptr + datasize;
  }

  uint8_t *Buffer::get_dataptr(int32_t &_datasize) const{ //TODO:: split to get a function for dataszie
    _datasize = db_end - ((uint8_t *) db + db->data_offset);
    return ((uint8_t *) db + db->data_offset);
  }

  uint8_t *Buffer::get_dataptr() const{
    return ((uint8_t *) db + db->data_offset);
  }

  int32_t Buffer::get_data_capacity() const {
    return db_end - ((uint8_t *) db + db->data_offset);
  }
  int Buffer::append_data_record(uint8_t *_st, int32_t _size) const{
    return db->append_data_record(_st, _size);
  }

  int32_t Buffer::get_num_data() const{
    return db->num_data;
  }

  uint8_t *Buffer::get_db_ptr() const {
    return (uint8_t *) db;
  }

  int32_t Buffer::get_db_size() const {
    return db_end - (uint8_t *) db;
  }

  block_id_t Buffer::get_block_id() const{
    return dbid;
  }

  int32_t Buffer::get_data_size() const {
    return db->dsize;
  }

  void Buffer::set_block_id(block_id_t _dbid){
    dbid = _dbid;
  }

  int32_t Buffer::get_db_size(int32_t _buffer_size){
    // must ensure that all buffers have the same size of meaningful data
    int num = sizeof(Buffer)/512 + 1; // number of 512B-sized blocks that Buffer object occupys.
    int32_t db_size = _buffer_size - 512*num - 512;
    return db_size;
  }

  // TODO: this is not good encapsulation as this function requires knowledge about DataBlock inside
  int32_t Buffer::get_data_capacity(int32_t _buffer_size){
    return DataBlock::get_data_capacity(get_db_size(_buffer_size));
  }

  Buffer *Buffer::create_buffer(int32_t _bsize, block_id_t _dbid){
    uint8_t *mem;
    try{
      mem = new uint8_t[_bsize];
    }catch(std::bad_alloc &ba){
      return NULL;
    }
    Buffer *buf = new (mem) Buffer(_bsize, _dbid);
    return buf;
  }

  void Buffer::set_valid_db_size(int32_t _vsize){
    valid_db_size = _vsize;
  }
  int32_t Buffer::get_valid_db_size() const{
    return valid_db_size;
  }
}

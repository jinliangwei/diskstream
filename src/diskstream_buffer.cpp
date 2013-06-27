
#include <string.h>
#include <new>

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

  DataBlock::DataBlock(int32_t _npartitions):
    num_partitions(_npartitions),
    partitions_list_offset(sizeof(DataBlock)),
    partitions_valid_offset(partitions_list_offset + sizeof(void *)*num_partitions),
    data_offset(partitions_valid_offset + sizeof(bool)*num_partitions),
    last_data_offset(data_offset),
    dsize(0),
    num_data(0){}

  int DataBlock::append_data_record(uint8_t *_st, uint32_t _size){
    last_data_offset = (_st - (uint8_t *) this);
    dsize += _size;
    ++num_data;
    return 0;
  }

  int DataBlock::partition(){
    //TODO: implement it to support multithreading
    memset(this + partitions_valid_offset, 0, sizeof(bool)*num_partitions);
    return 0;
  }

  Buffer::Buffer(uint32_t _bsize, int32_t _npartitions, block_id_t _dbid):
    bsize(_bsize),
    dbid(_dbid){
    uint8_t *db_ptr = ((uint8_t *) this) + sizeof(Buffer);
    db_ptr = align_ptr(db_ptr + 512, 512);
    db = new (db_ptr) DataBlock(_npartitions);
    // must ensure that all buffers have the same size of meaningful data
    int num = sizeof(Buffer)/512 + 1; // number of 512B-sized blocks that Buffer object occupys.
    datasize = bsize - 512*num - 512;
    db_end = db_ptr + datasize;
  }

  uint8_t *Buffer::get_dataptr(uint32_t &_datasize) const{ //TODO:: split to get a function for dataszie
    _datasize = db_end - ((uint8_t *) db + db->data_offset);
    return ((uint8_t *) db + db->data_offset);
  }

  uint8_t *Buffer::get_dataptr() const{
    return ((uint8_t *) db + db->data_offset);
  }
  int Buffer::append_data_record(uint8_t *_st, uint32_t _size) const{
    return db->append_data_record(_st, _size);
  }

  uint32_t Buffer::get_num_data() const{
    return db->num_data;
  }

  uint8_t *Buffer::get_db_ptr() const {
    return (uint8_t *) db;
  }

  uint32_t Buffer::get_db_size() const {
    return db_end - (uint8_t *) db;
  }

  block_id_t Buffer::get_block_id() const{
    return dbid;
  }

  uint32_t Buffer::get_data_size() const {
    return db->dsize;
  }

  void Buffer::set_block_id(block_id_t _dbid){
    dbid = _dbid;
  }
}

#ifndef __DISKSTREAM_BUFFER_HPP__
#define __DISKSTREAM_BUFFER_HPP__
#include <stdint.h>

#include "diskstream_types.hpp"

namespace diskstream{

  // to avoid circular reference
  class DataBlock;

  // buffer is assumed to sit at beginning of the allocated buffer-sized memory
  // only placement new can be used
  class Buffer{
  private:
    uint32_t bsize;                       // buffer size in bytes; should be multiple of MBs; max 3GB
    DataBlock *db;
    uint32_t datasize;                    // size of part of the buffer that holds meaningful data
                                          // this is not the size of the data stored; multiple of 512's
    uint8_t *db_end;
    block_id_t dbid;
  public:
    Buffer(uint32_t _bsize, int32_t _npartitions, block_id_t _dbid);
    uint8_t *get_dataptr(uint32_t &_datafsize) const;
    uint8_t *get_dataptr() const;
    int append_data_record(uint8_t *_st, uint32_t _size) const;
    uint32_t get_num_data() const;
    uint8_t *get_db_ptr() const;
    uint32_t get_db_size() const;
    block_id_t get_block_id() const;
    uint32_t get_data_size() const;
    void set_block_id(block_id_t _dbid);
  };

  class DataBlock {
  private:
    int32_t num_partitions;              // should only be power of 2
    uint32_t partitions_list_offset;     // offset from the beginning of this object
    uint32_t partitions_valid_offset;    // apply to all offsets
    uint32_t data_offset;
    uint32_t last_data_offset;
    uint32_t dsize;                     // total size of data
    uint32_t num_data;                  // total number of data records
    friend class Buffer;
  public:
    DataBlock(int32_t _npartitions);
    int append_data_record(uint8_t *_st, uint32_t _size);
    int partition();
  };


  class DataRecord{
  public:
    void *next;
    DataRecord();
    virtual ~DataRecord();
    virtual void *get_next();
    friend class DataBlock;
    friend class Buffer;
  };
}

#endif

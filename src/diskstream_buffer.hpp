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
    int32_t bsize;                       // buffer size in bytes; should be multiple of MBs; max 2GB
    DataBlock *db;
    int32_t datasize;                    // size of part of the buffer that holds meaningful data
                                          // this is not the size of the data actually stored;
                                          // multiple of 512's, size of everything in DataBlock
    uint8_t *db_end;
    block_id_t dbid;
    int32_t valid_db_size;                // DataBlock can be used to hold a chunk of data
                                          // this chunk of data might not use the DataBlock format
  public:
    Buffer(int32_t _bsize, block_id_t _dbid);
    uint8_t *get_dataptr(int32_t &_datafsize) const;
    uint8_t *get_dataptr() const;
    int append_data_record(uint8_t *_st, int32_t _size) const;
    int32_t get_num_data() const;
    uint8_t *get_db_ptr() const;
    int32_t get_db_size() const;
    block_id_t get_block_id() const;
    int32_t get_data_size() const;
    void set_block_id(block_id_t _dbid);
    int32_t get_data_capacity() const;
    void set_valid_db_size(int32_t _vsize);
    int32_t get_valid_db_size() const;
    int reset_data_block();

    static int32_t get_db_size(int32_t _buffer_size);
    static int32_t get_data_capacity(int32_t _buffer_size);
    static Buffer *create_buffer(int32_t _bsize, block_id_t _dbid);
  };

  class DataBlock {
  private:
    int32_t data_offset;
    int32_t last_data_offset;
    int32_t dsize;                     // total size of data
    int32_t num_data;                  // total number of data records
    friend class Buffer;
  public:
    DataBlock();
    int append_data_record(uint8_t *_st, int32_t _size);
    int reset();
    static int32_t get_data_capacity(int32_t _db_size);
  };
}

#endif

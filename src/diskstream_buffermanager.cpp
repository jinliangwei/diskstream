
#include <vector>
#include <iostream>

#include "diskstream_buffermanager.hpp"

namespace diskstream{

  const int KBlockSize = 512;

  BufferManager::BufferManager(int64_t _total_mem_size, int32_t _buff_size, DiskIOThread *_diskio):
    mgrstate(BufMgrInit),
    total_mem_size(_total_mem_size),
    free_mem_size(total_mem_size),
    buff_size(_buff_size),
    diskio(_diskio),
    num_total_buffers(total_mem_size/buff_size),
    num_assigned_buffers(0),
    num_io_buffers(0),
    load_iter(0),
    max_iter(0),
    putback_iter(0),
    putback_id(-1),
    next_load_db_id(0),
    max_db_id(0),
    extern_read_size(Buffer::get_db_size(buff_size) - KBlockSize),
    curr_file_idx(0),
    access_iter(0),
    access_db_id(-1),
    access_file_idx(0),
    extern_write_offset(0){
    diskio_cid = diskio->register_client();
  }

  BufferManager::~BufferManager(){
    std::map<Buffer *, bool>::iterator iter;
    for(iter = buffers.begin(); iter != buffers.end(); ++iter){
      if(iter->second){
        delete iter->first;
        buffers[iter->first] = false;
      }
    }
  }

  int BufferManager::sequen_init_all_reads(){
    std::cout << "sequen_init_all_reads()" << std::endl;
    std::vector<int32_t> fsizes = diskio->get_all_sizes(base);
    int32_t num_files = fsizes.size(), i, buffers_max = 0;

    // buffers_max is the maximum number of buffers to be used to load all data in
    for(i = 0; i < num_files; ++i){
      buffers_max += (fsizes[i] + Buffer::get_db_size(buff_size) - 1)/Buffer::get_db_size(buff_size);
//      std::cout << "Buffer::get_db_size(buff_size) = " << Buffer::get_db_size(buff_size)
//                << " fsizes[i] = " << fsizes[i] << std::endl;

    }

    max_db_id = buffers_max - 1;
    putback_id = max_db_id;

    if(next_load_db_id > max_db_id){
      next_load_db_id = 0;
      ++load_iter;
      return 0;
    }

    while(free_buff_q.size() > 0 && next_load_db_id <= putback_id){
      Buffer *buff = free_buff_q.front();
      buff->set_block_id(next_load_db_id);
      diskio->sche_read(diskio_cid, buff, base.c_str());
      ++next_load_db_id;
      ++num_io_buffers;
      free_buff_q.pop();
    }
    while(free_mem_size >= buff_size && next_load_db_id <= putback_id){
      Buffer *buff = Buffer::create_buffer(buff_size, next_load_db_id);
      buffers[buff] = true;
      diskio->sche_read(diskio_cid, buff, base.c_str());
      ++next_load_db_id;
      ++num_io_buffers;
      free_mem_size -= buff_size;
    }
    if(next_load_db_id > putback_id){
      next_load_db_id = 0;
      ++load_iter;
    }
    return 0;
  }

  // return 0 if successfully scheduled
  // return negative if failure
  int BufferManager::sequen_sched_read(Buffer *_buff){

    _buff->set_block_id(next_load_db_id);
    int suc = diskio->sche_read(diskio_cid, _buff, base.c_str());
    if(suc < 0) return -1;
    ++next_load_db_id;
    if(next_load_db_id > max_db_id){
      ++load_iter;
      next_load_db_id = 0;
    }
    return 0;
  }

  int BufferManager::sequen_sched_writeread(Buffer *_buff){
    int suc;
    suc = diskio->sche_write_read(diskio_cid, _buff, base.c_str(), next_load_db_id);
    if(suc < 0) return -1;
    ++next_load_db_id;
    if(next_load_db_id > max_db_id){
      ++load_iter;
      next_load_db_id = 0;
    }
    return 0;
  }

  int BufferManager::sequen_extern_init_all_reads(){
    fsizes = DiskIOThread::get_all_sizes(seqextern_fullnames);
    fmax_db_ids.resize(fsizes.size());
    int32_t i;
    for(i = 0; i < (int32_t) fmax_db_ids.size(); ++i){
      fmax_db_ids[i] = -1;
    }
    int32_t num_files = fsizes.size(), buffers_max = 0;
    curr_file_idx = 0;
    // buffers_max is the maximum number of buffers to be used to load all data in

    int32_t db_size = Buffer::get_db_size(buff_size) - 1;

    buffers_max = (fsizes[curr_file_idx] + db_size - 1)/db_size;
    max_db_id = buffers_max - 1;
    fmax_db_ids[curr_file_idx] = max_db_id;
    next_load_db_id = 0;
    bool used_all = false, read_all = false;

    while((!free_buff_q.empty() || free_mem_size >= buff_size) && !read_all){
      while(curr_file_idx < num_files && next_load_db_id <= max_db_id && !used_all){

        if(!free_buff_q.empty()){
          Buffer *buff = free_buff_q.front();
          buff->set_block_id(next_load_db_id);
          diskio->sche_read_extern(diskio_cid, buff, seqextern_fullnames[curr_file_idx].c_str(),
                                   extern_read_size*next_load_db_id, extern_read_size);
          ++num_io_buffers;
          ++next_load_db_id;
          if(next_load_db_id > max_db_id){
            ++curr_file_idx;
            if(curr_file_idx >= num_files){
              read_all = true;
              break;
            }
            next_load_db_id = 0;
            buffers_max = (fsizes[curr_file_idx] + db_size - 1)/db_size;
            max_db_id = buffers_max - 1;
            fmax_db_ids[curr_file_idx] = max_db_id;
          }
          free_buff_q.pop();
        }else if(free_mem_size >= buff_size){
          Buffer *buff = Buffer::create_buffer(buff_size, next_load_db_id);
          free_mem_size -= buff_size;
          buffers[buff] = true;
          diskio->sche_read_extern(diskio_cid, buff, seqextern_fullnames[curr_file_idx].c_str(),
                                    extern_read_size*next_load_db_id, extern_read_size);
          ++num_io_buffers;
          ++next_load_db_id;
          if(next_load_db_id > max_db_id){
            ++curr_file_idx;
            if(curr_file_idx >= num_files){
              read_all = true;
              break;
            }
            next_load_db_id = 0;
            buffers_max = (fsizes[curr_file_idx] + db_size - 1)/db_size;
            max_db_id = buffers_max - 1;
            fmax_db_ids[curr_file_idx] = max_db_id;
          }
        }else{
          used_all = true;
        }
      }
    }
    return 0;
  }

  int BufferManager::sequen_extern_sched_read(Buffer *_buff){
    int suc;
    suc = diskio->sche_read_extern(diskio_cid, _buff, seqextern_fullnames[curr_file_idx].c_str(),
                                   extern_read_size*next_load_db_id, extern_read_size);
    if(suc < 0) return -1;
    ++next_load_db_id;
    if(next_load_db_id > max_db_id){
      ++curr_file_idx;
      next_load_db_id = 0;
      if(curr_file_idx < (int32_t) seqextern_fullnames.size()){
        int32_t buffers_max = (fsizes[curr_file_idx] + buff_size - 1)/buff_size;
        max_db_id = buffers_max - 1;
        fmax_db_ids[curr_file_idx] = max_db_id;
      }
    }
    return 0;
  }

  int BufferManager::init(std::string _base){
    base = _base;
    mgrstate = BufMgrInit;
    diskio->enable(diskio_cid);
    return 0;
  }

  int BufferManager::init_sequen(std::string _base, int32_t _db_st, int32_t _max_iter){
    if(_db_st < 0) return -1;
    if(_max_iter == 0) return -1;
    mgrstate = BufMgrSequen;
    base = _base;
    max_iter = (_max_iter > 0) ? _max_iter : 0;
    load_iter = 0;
    putback_iter = 0;
    next_load_db_id = _db_st;
    access_iter = 0;
    access_db_id = _db_st -1;
    diskio->enable(diskio_cid);
    return sequen_init_all_reads();
  }

  int BufferManager::init_sequen_extern(std::vector<std::string> _fullnames){

    seqextern_fullnames = _fullnames;
    access_db_id = -1;
    access_file_idx = 0;
    mgrstate = BufMgrSequenExtern;
    diskio->enable(diskio_cid);
    return sequen_extern_init_all_reads();
  }

  int BufferManager::init_write_extern(std::string _fullname){

    base = _fullname;
    extern_write_offset = 0;
    mgrstate = BufMgrWriteExtern;
    diskio->enable(diskio_cid);
    return 0;
  }


  Buffer *BufferManager::get_buffer_init(){

    if(free_mem_size >= buff_size){
      Buffer *buff;
      buff = Buffer::create_buffer(buff_size, 0); // I don't care about dbid, user needs to worry
                                                  // about it
      buffers[buff] = true;
      free_mem_size -= buff_size;
      ++num_assigned_buffers;
      std::cout << "created new buffer and return" << std::endl;
      return buff;
    }else if(!free_buff_q.empty()){
      Buffer *buff;
      buff = free_buff_q.front();
      buff->reset_data_block();
      free_buff_q.pop();
      ++num_assigned_buffers;
      return buff;
    }else if(num_assigned_buffers < num_total_buffers){
      Buffer *buff;
      EMsgType type;
      int32_t bytes;
      int suc;
      while(num_io_buffers > 0){
        suc = diskio->get_response(diskio_cid, buff, type, bytes);
        if(suc < 0) return NULL;
        --num_io_buffers;
        assert(type == DiskIOWriteDone || type == DiskIOWriteExternalDone);
        ++num_assigned_buffers;
        buff->reset_data_block();
        return buff;
      }
    }else{
      return NULL;
    }
    return NULL;
  }

  /*
   * BufferManager schedules read when both of the two conditions are met:
   * 1) there are free buffers
   * 2) the putback position is ahead of the load position
   *
   * During initialization, BufferManager schedules as many reads as possible until one of the two
   * conditions does not meet. Then during the operations, BufferManager tries to schedule reads if
   * either condition could be met due to the operation.
   * */

  Buffer *BufferManager::get_buffer_sequen(){
    int suc;
    Buffer *buff;
    EMsgType type;
    int32_t bytes;
    while(num_io_buffers > 0){
      suc = diskio->get_response(diskio_cid, buff, type, bytes);
      assert(suc == 0);
      if(suc < 0) return NULL;
      //std::cout << "get_buffer_sequen: buffer->get_block_id() = " << buff->get_block_id()
      //          << " access_db_id = " << access_db_id << std::endl;
      //std::cout << "diskio return type is " << type << std::endl;
      assert(type == DiskIOReadDone || type == DiskIOWriteReadDone);
      --num_io_buffers;
      ++access_db_id;
      if(access_db_id > max_db_id){
        ++access_iter;
        access_db_id = 0;
      }
      return buff;
    }
    return NULL;
  }

  Buffer *BufferManager::get_buffer_sequen_extern(){
    int suc;
    Buffer *buff;
    EMsgType type;
    int32_t bytes;
    while(num_io_buffers > 0){
      suc = diskio->get_response(diskio_cid, buff, type, bytes);
      if(suc < 0) return NULL;
      --num_io_buffers;
      ++access_db_id;
      //assert(fmax_db_ids[access_file_idx] >= 0);
      if(access_db_id > fmax_db_ids[access_file_idx]){
         access_db_id = 0;
         ++access_file_idx;
      }
      assert(type == DiskIOReadExternalDone);
      assert(bytes > 0);

      uint8_t *db_ptr = buff->get_db_ptr();
      db_ptr[bytes] = '\0';
      buff->set_valid_db_size(bytes + 1);
      return buff;
    }
    return NULL;
  }

  int BufferManager::putback_init(Buffer *_buff, EBufMgrBufPolicy _policy){
    --num_assigned_buffers;

    int suc;
    switch(_policy){
    case BufPolicyAbort:
      free_buff_q.push(_buff);
      break;
    case BufPolicyWriteBack:
      suc = diskio->sche_write(diskio_cid, _buff, base.c_str());
      if(suc < 0) return -1;
      ++num_io_buffers;
      break;
    default:
      assert(1);
      break;
    }
    return 0;
  }

  int BufferManager::putback_write_extern(Buffer *_buff, EBufMgrBufPolicy _policy){
      --num_assigned_buffers;

      int suc;
      switch(_policy){
      case BufPolicyAbort:
        free_buff_q.push(_buff);
        break;
      case BufPolicyWriteBack:
        suc = diskio->sche_write_extern(diskio_cid, _buff, base.c_str(), extern_write_offset,
                                        _buff->get_valid_db_size());
        if(suc < 0) return -1;
        extern_write_offset += _buff->get_valid_db_size();
        ++num_io_buffers;
        break;
      default:
        assert(1);
        break;
      }
      return 0;
    }

  // TODO: add EPutBackPolicy, depending on putback policy to decide if putback_id should be
  // incremented
  int BufferManager::putback_sequen(Buffer *_buff, EBufMgrBufPolicy _policy){
    // current policy: increment putback_id if they are contiguous, do not increment otherwise
    if(putback_id == max_db_id){
      if(_buff->get_block_id() == 0){
        putback_id = 0;
        ++putback_iter;
      }
    }else{
      if(_buff->get_block_id() == (putback_id + 1)){
        ++putback_id;
      }
    }

    --num_assigned_buffers;
    int suc;
    switch(_policy){
    case BufPolicyAbort:
      if(load_iter >= max_iter){
        free_buff_q.push(_buff);
        return 0;
      }
      // the current buffer is free, schedule it for read
      suc = sequen_sched_read(_buff);
      assert(suc == 0);
      if(suc < 0) // this read must succeed
        return -1;
      ++num_io_buffers;
      break;
    case BufPolicyWriteBack:
      //std::cout << "putback_sequen, load_iter = "
      if(load_iter <= (max_iter - 1)){
        suc = sequen_sched_writeread(_buff);
        if(suc < 0) return -1;
        ++num_io_buffers;
      }else{
        suc = diskio->sche_write(diskio_cid, _buff, base.c_str());
        if(suc < 0) return -1;
        ++num_io_buffers;
      }
      break;
    default:
      assert(1);
      break;
    }
    return 0;
  }

  int BufferManager::putback_sequen_extern(Buffer *_buff, EBufMgrBufPolicy _policy){
    if(_policy != BufPolicyAbort) return -1;
    --num_assigned_buffers;
    if((uint32_t) curr_file_idx < seqextern_fullnames.size()){
      int suc = sequen_extern_sched_read(_buff);
      if(suc < 0) return -1;
      ++num_io_buffers;
    }else{
      free_buff_q.push(_buff);
    }
    return 0;
  }

  Buffer *BufferManager::get_buffer(){
    switch(mgrstate){
    case BufMgrInit:
    case BufMgrWriteExtern:
      return get_buffer_init();
    case BufMgrSequen:
      return get_buffer_sequen();
    case BufMgrSequenExtern:
      return get_buffer_sequen_extern();
    default:
      assert(1);
      break;
    }
    return NULL;
  }

  int BufferManager::putback(Buffer *_buf, EBufMgrBufPolicy _policy){
    switch(mgrstate){
    case BufMgrInit:
      return putback_init(_buf, _policy);
    case BufMgrSequen:
      return putback_sequen(_buf, _policy);
    case BufMgrSequenExtern:
      return putback_sequen_extern(_buf, _policy);
    case BufMgrWriteExtern:
      return putback_write_extern(_buf, _policy);
    default:
      assert(1);
      break;
    }
    return 0;
  }

  int BufferManager::stop(){
    int suc;
    Buffer *buff;
    EMsgType type;
    int32_t bytes;
    diskio->cancel(diskio_cid);
    while(num_io_buffers > 0){
      suc = diskio->get_response(diskio_cid, buff, type, bytes);
      if(suc < 0) return -1;
//      std::cout << "BufferManager::stop() : get_response " << (void *) buff
//                << " type = " << type << std::endl;
      --num_io_buffers;
      free_buff_q.push(buff);
    }
    return 0;
  }

  bool BufferManager::reached_end(){
    switch(mgrstate){
    case BufMgrSequen:
//      std::cout << "reached_end() : "
//                <<"access_iter = " << access_iter
//                << " access_db_id = " << access_db_id
//                << " max_db_id = " << max_db_id << std::endl;
      if(max_iter > 0 && access_iter == (max_iter - 1) && access_db_id == max_db_id){
        return true;
      }else{
        return false;
      }

    case BufMgrSequenExtern:
     if((uint32_t) access_file_idx == seqextern_fullnames.size() - 1
         && access_db_id == fmax_db_ids[access_file_idx]){
        return true;
      }else{
        return false;
      }
    default:
      break;
    }
    return false;
  }

  int BufferManager::add_free_mem(int32_t _mem_size){
    total_mem_size += _mem_size;
    free_mem_size += _mem_size;
    switch(mgrstate){
    case BufMgrSequen:
      while(free_mem_size >= buff_size && load_iter < putback_iter && load_iter < max_iter){
        Buffer *buff = Buffer::create_buffer(buff_size, next_load_db_id);
        buffers[buff] = true;
        diskio->sche_read(diskio_cid, buff, base.c_str());
        ++next_load_db_id;
        if(next_load_db_id > max_db_id){
          ++load_iter;
          next_load_db_id = 0;
        }
        ++num_io_buffers;
      }
      break;
    case BufMgrSequenExtern:
      while(free_mem_size >= buff_size && (uint32_t) curr_file_idx < seqextern_fullnames.size()){
        Buffer *buff = Buffer::create_buffer(buff_size, next_load_db_id);
        buffers[buff] = true;
        diskio->sche_read_extern(diskio_cid, buff, seqextern_fullnames[curr_file_idx].c_str(),
                                 buff_size*next_load_db_id, buff_size);
        ++next_load_db_id;
        if(next_load_db_id > max_db_id){
          ++curr_file_idx;
          next_load_db_id = 0;
        }
        ++num_io_buffers;
      }
      break;
    default:
      break;
    }
    return 0;
  }

  int BufferManager::add_free_buffs(std::vector<Buffer *> _buffs){
    num_total_buffers += _buffs.size();
    int32_t i;
    switch(mgrstate){
    case BufMgrInit:
    case BufMgrWriteExtern:
      for(i = 0; (uint32_t) i < _buffs.size(); ++i){
        free_buff_q.push(_buffs[i]);
        buffers[_buffs[i]] = true;
      }
      break;
    case BufMgrSequen:
      i = 0;
      while((uint32_t) i < _buffs.size() && load_iter < putback_iter && load_iter < max_iter){
        Buffer *buff = _buffs[i];
        buffers[buff] = true;
        diskio->sche_read(diskio_cid, buff, base.c_str());
        ++next_load_db_id;
        if(next_load_db_id > max_db_id){
          ++load_iter;
          next_load_db_id = 0;
        }
        ++num_io_buffers;
        ++i;
      }
      for(; (uint32_t) i < _buffs.size(); ++i){
        free_buff_q.push(_buffs[i]);
        buffers[_buffs[i]] = true;
      }
      break;
    case BufMgrSequenExtern:
      i = 0;
      while((uint32_t) i < _buffs.size() && (uint32_t) curr_file_idx < seqextern_fullnames.size()){
        Buffer *buff = _buffs[i];
        buffers[buff] = true;
        diskio->sche_read_extern(diskio_cid, buff, seqextern_fullnames[curr_file_idx].c_str(),
            buff_size*next_load_db_id, buff_size);
        ++next_load_db_id;
        if(next_load_db_id > max_db_id){
          ++curr_file_idx;
          next_load_db_id = 0;
        }
        ++num_io_buffers;
        ++i;
      }
      for(;(uint32_t) i < _buffs.size(); ++i){
        free_buff_q.push(_buffs[i]);
        buffers[_buffs[i]] = true;
      }
      break;
    }
    return 0;
  }

  int32_t BufferManager::get_num_free_buffs(){
    return free_buff_q.size();
  }

  int32_t BufferManager::get_free_mem_size(){
    return free_mem_size;
  }

  int BufferManager::release_all_free_buffs(std::vector<Buffer *> &_buffs, int32_t &_mem_size){
    int32_t num_free_buffers = free_buff_q.size() , i = 0;
    _buffs.resize(num_free_buffers);
    while(!free_buff_q.empty()){
      Buffer *buff = free_buff_q.front();
      buff->reset_data_block();
      free_buff_q.pop();
      _buffs[i] = buff;
      buffers[buff] = false;
      ++i;
    }
    _mem_size = free_mem_size;
    total_mem_size -= _mem_size;
    free_mem_size = 0;
    return 0;
  }
}


#include <string.h>
#include <assert.h>
#include <sstream>
#include <iostream>

#include "diskstream_lda_executor.hpp"

namespace diskstream {
  LdaExecutorInit::LdaExecutorInit():
       extern_data_loader(NULL),
       task_buffer_mgr(NULL),
       data_buffer_mgr(NULL),
       extern_data_writer(NULL),
       zmq_ctx(NULL),
       diskio(NULL),
       textparser(NULL),
       membudget(0),
       buffsize(0),
       num_total_tasks(0),
       num_topics(0),
       max_line_len(0),
       num_total_buffs(0),
       num_task_buffs(0),
       num_data_buffs(0),
       num_tasks_per_buff(0),
       num_total_task_buffs(0),
       num_my_init_data_buffs(0),
       my_init_data_buffs(NULL),
       num_total_data_buffs(0),
       num_my_task_buffs(0),
       my_task_buffs(NULL),
       curr_task_st(0),
       curr_task_ed(0){}

   LdaExecutorInit::~LdaExecutorInit(){
     if(extern_data_loader != NULL){
       delete extern_data_loader;
       extern_data_loader = NULL;
     }
     if(task_buffer_mgr != NULL){
       delete task_buffer_mgr;
       task_buffer_mgr = NULL;
     }
     if(data_buffer_mgr != NULL){
       delete data_buffer_mgr;
       data_buffer_mgr = NULL;
     }
     if(extern_data_writer != NULL){
       delete extern_data_writer;
       extern_data_writer = NULL;
     }
     /*
      * has to destroy diskio before destroying zmq_ctx
      * because it has to close all sockets opened with zmq_ctx before terminating zmq_ctx
      * otherwise, it hangs
      * */
     if(diskio != NULL){
       delete diskio;
       diskio = NULL;
     }
     if(zmq_ctx != NULL){
       delete zmq_ctx;
       zmq_ctx = NULL;
     }
     if(textparser != NULL){
       delete textparser;
       textparser = NULL;
     }

     if(my_init_data_buffs != NULL){
       delete my_init_data_buffs;
       my_init_data_buffs = NULL;
     }

     if(my_task_buffs != NULL){
       delete my_task_buffs;
       my_task_buffs = NULL;
     }
   }

   int LdaExecutorInit::init_task_buffers(){
     MemChunkAccesser task_buff_acc;
     int32_t task_id = 0;
     Buffer *buffer = task_buffer_mgr->get_buffer();
     bool last_buff_putback = false;
     int suc;
     if(buffer == NULL) return -1;

     suc = task_buffer_mgr->init(internbase + ".tasks");
     if(suc < 0) return -1;

     int32_t task_buffs_idx = 0;
     curr_task_st = 0;
     num_my_task_buffs = 0;
     buffer->set_block_id(task_buffs_idx);
     my_task_buffs[task_buffs_idx] = buffer;
     ++num_my_task_buffs;
     ++task_buffs_idx;

     task_buff_acc.set_mem(buffer->get_dataptr(), buffer->get_data_capacity());
     while(task_id < (int32_t) num_total_tasks){
       Word *w;
       try{
         w = new (task_buff_acc.curr_ptr()) Word(task_buff_acc.avai_size(), task_id, num_topics);
         ++task_id;
         buffer->append_data_record(task_buff_acc.curr_ptr(), w->size());
         int suc = task_buff_acc.advance(w->size());
         assert(suc == 0);
       }catch(EDataCreateErr &e){
         // Add each task buffers got from task_buff_mgr to my_task_buffs array
         // until the buffers held by task_buff_mgr reaches its minimum.
         // Put the last buffer back before getting a new buffer.
         // If so, set last_buff_putback to true and subtract one 1 from num_my_task_buffs
         // so I know I'm not holding the last buffer.
         if(e == DataCreateInsufficientMem){
           if(num_task_buffs <= (num_my_task_buffs + KMinTaskBuffsMgr)
            && num_total_task_buffs > num_task_buffs){ // do not put back if memory is sufficient to hold
                                                         // all tasks
             suc = task_buffer_mgr->putback(buffer, BufPolicyWriteBack);
             assert(suc == 0);
             if(suc < 0) return -1;
             buffer = task_buffer_mgr->get_buffer();
             assert(buffer != NULL);
             if(buffer == NULL) return -1;
             buffer->set_block_id(task_buffs_idx);
             task_buff_acc.set_mem(buffer->get_dataptr(), buffer->get_data_capacity());
             ++task_buffs_idx;
             last_buff_putback = true;
           }else{
             buffer = task_buffer_mgr->get_buffer();
             if(buffer == NULL) return -1;
             buffer->set_block_id(task_buffs_idx);
             task_buff_acc.set_mem(buffer->get_dataptr(), buffer->get_data_capacity());
             my_task_buffs[task_buffs_idx] = buffer;
             ++task_buffs_idx;
             ++num_my_task_buffs;
           }
         }else{
           assert(1);
         }
       }
     }
     num_total_task_buffs = task_buffs_idx;
     if(last_buff_putback){
       suc = task_buffer_mgr->putback(buffer, BufPolicyWriteBack);
       assert(suc == 0);
       --num_my_task_buffs;
     }
     int32_t ti;
     curr_task_ed = 0;
     for(ti = 0; ti < num_my_task_buffs; ++ti){
       curr_task_ed += my_task_buffs[ti]->get_data_size()/Word::usize(num_topics);
     }
     task_buffer_mgr->stop();
     return 0;
   }

   int LdaExecutorInit::init_data_buffers(){
     MemChunkAccesser data_buff_acc;
     Buffer *extern_buff, *data_buff = data_buffer_mgr->get_buffer();
     int32_t datasize, line_len;
     int suc;
     char *linebuf = new char[max_line_len];
     Document *doc;
     bool last_buff_putback = false;

     suc = extern_data_loader->init_sequen_extern(extern_data_paths);
     if(suc < 0) return -1;

     suc = data_buffer_mgr->init(internbase + ".data");
     if(suc < 0) return -1;

     int32_t data_buffs_idx = 0;
     data_buff_acc.set_mem(data_buff->get_dataptr(), data_buff->get_data_capacity());
     data_buff->set_block_id(data_buffs_idx);

     my_init_data_buffs[data_buffs_idx] = data_buff;
     ++data_buffs_idx;
     num_my_init_data_buffs = 1;

     assert(!extern_data_loader->reached_end());

     while(!extern_data_loader->reached_end()){
       extern_buff = extern_data_loader->get_buffer();
       assert(extern_buff != NULL);
       datasize = extern_buff->get_valid_db_size();
       // datasize includes the ending '\0', so we need to subtract 1 from it
       suc = textparser->set_textbuf((char *) extern_buff->get_db_ptr(), datasize - 1);
       line_len = textparser->get_next(linebuf);

       while(line_len > 0){
         try{
           doc = new (data_buff_acc.curr_ptr()) Document(data_buff_acc.avai_size(),
                                                         std::string(linebuf), num_topics);
           data_buff->append_data_record(data_buff_acc.curr_ptr(), doc->size());
           suc = data_buff_acc.advance(doc->size());
           assert(suc == 0);
           line_len = textparser->get_next(linebuf);
         }catch(EDataCreateErr &e){
           if(e == DataCreateInsufficientMem){
             if(num_data_buffs <= (KMinDataBuffsMgr + num_my_init_data_buffs)){
               suc = data_buffer_mgr->putback(data_buff, BufPolicyWriteBack);
               assert(suc == 0);
               if(suc < 0) return -1;
               std::cout << "put back data_buff = " << (void *) data_buff << std::endl;
               data_buff = data_buffer_mgr->get_buffer();
               assert(data_buff != NULL);
               if(data_buff == NULL) return -1;
               data_buff->set_block_id(data_buffs_idx);
               data_buff_acc.set_mem(data_buff->get_dataptr(), data_buff->get_data_capacity());
               ++data_buffs_idx;
               last_buff_putback = true;
             }else{
               data_buff = data_buffer_mgr->get_buffer();
               assert(data_buff != NULL);
               if(data_buff == NULL) return -1;
               data_buff->set_block_id(data_buffs_idx);
               data_buff_acc.set_mem(data_buff->get_dataptr(), data_buff->get_data_capacity());
               my_init_data_buffs[data_buffs_idx] = data_buff;
               ++data_buffs_idx;
               ++num_my_init_data_buffs;
             }
           }else{
             line_len = textparser->get_next(linebuf);
             std::cout << "WARNING!!!! empty line!" << std::endl;
             continue;
           }
         }
       }
       suc = extern_data_loader->putback(extern_buff, BufPolicyAbort);
       assert(suc == 0);
     }
     line_len = textparser->get_last(linebuf);
     if(line_len > 0){
       try{
         doc = new (data_buff_acc.curr_ptr()) Document(data_buff_acc.avai_size(),
             std::string(linebuf), num_topics);
         data_buff->append_data_record(data_buff_acc.curr_ptr(), doc->size());
         suc = data_buff_acc.advance(doc->size());
         assert(suc == 0);
       }catch(EDataCreateErr &e){
         if(e == DataCreateInsufficientMem){
           if(num_data_buffs <= (KMinDataBuffsMgr + num_my_init_data_buffs)){
             suc = data_buffer_mgr->putback(data_buff, BufPolicyWriteBack);
             assert(suc == 0);
             if(suc < 0) return -1;
             std::cout << "put back data_buff = " << (void *) data_buff << std::endl;
             data_buff = data_buffer_mgr->get_buffer();
             assert(data_buff != NULL);
             if(data_buff == NULL) return -1;
             data_buff->set_block_id(data_buffs_idx);
             data_buff_acc.set_mem(data_buff->get_dataptr(), data_buff->get_data_capacity());
             ++data_buffs_idx;
             last_buff_putback = true;
           }else{
             data_buff = data_buffer_mgr->get_buffer();
             assert(data_buff != NULL);
             if(data_buff == NULL) return -1;
             data_buff->set_block_id(data_buffs_idx);
             data_buff_acc.set_mem(data_buff->get_dataptr(), data_buff->get_data_capacity());
             my_init_data_buffs[data_buffs_idx] = data_buff;
             ++data_buffs_idx;
             ++num_my_init_data_buffs;
           }
         }
       }
     }
     num_total_data_buffs = data_buffs_idx;
     if(last_buff_putback){
       suc = data_buffer_mgr->putback(data_buff, BufPolicyWriteBack);
       assert(suc == 0);
       --num_my_init_data_buffs;
     }
     return 0;
   }

   int LdaExecutorInit::output_task_buffers(){
     char *templine = new char[max_line_len];
     int suc = 0;
     int32_t tbuff_idx;
     MemChunkAccesser buff_acc, extern_buff_acc;

     extern_data_writer->init_write_extern(internbase + ".tasks.extern");
     Buffer *extern_buff = extern_data_writer->get_buffer();
     extern_buff_acc.set_mem(extern_buff->get_db_ptr(), extern_buff->get_db_size());

     if(num_my_task_buffs < num_total_task_buffs){
       task_buffer_mgr->init_sequen(internbase + ".tasks", num_my_task_buffs, 1);
     }

     int32_t num_task_buffs_accessed = 0;
     Word *w;
     do{
       for(tbuff_idx = 0; tbuff_idx < num_my_task_buffs; ++tbuff_idx){
         buff_acc.set_mem(my_task_buffs[tbuff_idx]->get_dataptr(), my_task_buffs[tbuff_idx]->get_data_size());
         while(buff_acc.avai_size() > 0){
           w = (Word *) buff_acc.curr_ptr();
           sprintf(templine, "%d\n", w->get_task_id());
           if((uint) extern_buff_acc.avai_size() < strlen(templine)){
             extern_buff->set_valid_db_size(extern_buff->get_db_size() - extern_buff_acc.avai_size());
             suc = extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
             assert(suc == 0);
             extern_buff = extern_data_writer->get_buffer();
             assert(extern_buff != NULL);
             extern_buff_acc.set_mem(extern_buff->get_dataptr(), extern_buff->get_data_capacity());
           }
           memcpy(extern_buff_acc.curr_ptr(), templine, strlen(templine));
           suc = extern_buff_acc.advance(strlen(templine));
           assert(suc == 0);
           suc = buff_acc.advance(w->size());
           assert(suc == 0);
         }
       }

       num_task_buffs_accessed += num_my_task_buffs;

       if(num_task_buffs_accessed < num_total_task_buffs){
         for(tbuff_idx = 0; tbuff_idx < num_my_task_buffs; ++tbuff_idx){
           task_buffer_mgr->putback(my_task_buffs[tbuff_idx], BufPolicyWriteBack);
         }
         num_my_task_buffs = 0;

         if(task_buffer_mgr->reached_end()) break;

         while(!task_buffer_mgr->reached_end() && num_my_task_buffs < num_task_buffs){
           my_task_buffs[num_my_task_buffs] = task_buffer_mgr->get_buffer();
           assert(my_task_buffs[num_my_task_buffs] != NULL);
           ++num_my_task_buffs;
         }
       }
     }while(num_task_buffs_accessed < num_total_task_buffs);

     extern_buff->set_valid_db_size(extern_buff->get_db_size() - extern_buff_acc.avai_size());
     extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
     extern_data_writer->stop();
     delete templine;
     return 0;
   }

   int LdaExecutorInit::output_data_buffers(){
     char *templine = new char[max_line_len];
     MemChunkAccesser buff_acc, extern_buff_acc;
     extern_data_writer->init_write_extern(internbase + ".data.extern");

     Buffer *extern_buff = extern_data_writer->get_buffer();
     extern_buff_acc.set_mem(extern_buff->get_db_ptr(), extern_buff->get_db_size());

     int suc;
     int32_t dbuff_idx;
     Document *doc;
     for(dbuff_idx = 0; dbuff_idx < num_my_init_data_buffs; ++dbuff_idx){
       buff_acc.set_mem(my_init_data_buffs[dbuff_idx]->get_dataptr(),
                       my_init_data_buffs[dbuff_idx]->get_data_size());
       while(buff_acc.avai_size() > 0){
         doc = (Document *) buff_acc.curr_ptr();
         int32_t num_uwords;
         const int32_t *uwords;
         const int32_t *wcnts;
         uwords = doc->get_task_list(num_uwords);
         wcnts = doc->get_wcnt_list(num_uwords);
         sprintf(templine, "%d", num_uwords);
         int32_t i;
         for(i = 0; i < num_uwords; ++i){
           sprintf(templine + strlen(templine), " %d:%d", uwords[i], wcnts[i]);
         }
         int32_t len = strlen(templine);
         templine[len] = '\n';
         ++len;
         if(extern_buff_acc.avai_size() < len){
           std::cout << "Not enough memory for external data buffer" << std::endl;
           extern_buff->set_valid_db_size(extern_buff->get_db_size() - extern_buff_acc.avai_size());
           extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
           extern_buff = extern_data_writer->get_buffer();
           assert(extern_buff != NULL);
           extern_buff_acc.set_mem(extern_buff->get_db_ptr(), extern_buff->get_db_size());
         }
         memcpy(extern_buff_acc.curr_ptr(), templine, len);
         extern_buff_acc.advance(len);
         suc = buff_acc.advance(doc->size());
         assert(suc == 0);
       }
     }

     if(num_my_init_data_buffs < num_total_data_buffs){
       data_buffer_mgr->stop();
       data_buffer_mgr->init_sequen(internbase + ".data", num_my_init_data_buffs, 1);
       Buffer *data_buff;
       while(!data_buffer_mgr->reached_end()){
         data_buff = data_buffer_mgr->get_buffer();
         buff_acc.set_mem(data_buff->get_dataptr(),
                              data_buff->get_data_size());
         while(buff_acc.avai_size() > 0){
           doc = (Document *) buff_acc.curr_ptr();
           int32_t num_uwords;
           const int32_t *uwords;
           const int32_t *wcnts;
           uwords = doc->get_task_list(num_uwords);
           wcnts = doc->get_wcnt_list(num_uwords);
           sprintf(templine, "%d", num_uwords);
           int32_t i;
           for(i = 0; i < num_uwords; ++i){
             sprintf(templine + strlen(templine), " %d:%d", uwords[i], wcnts[i]);
           }
           int32_t len = strlen(templine);
           templine[len] = '\n';
           ++len;
           if(extern_buff_acc.avai_size() < len){
             extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
             extern_buff = extern_data_writer->get_buffer();
             extern_buff_acc.set_mem(extern_buff->get_db_ptr(), extern_buff->get_db_size());
           }
           memcpy(extern_buff_acc.curr_ptr(), templine, len);
           extern_buff->append_data_record(extern_buff_acc.curr_ptr(), len);
           extern_buff_acc.advance(len);
           suc = buff_acc.advance(doc->size());
           assert(suc == 0);
         }
       }
     }
     extern_buff->set_valid_db_size(extern_buff->get_db_size() - extern_buff_acc.avai_size());
     extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
     extern_data_writer->stop();
     return 0;
   }

   int LdaExecutorInit::init(int64_t _membudget, int32_t _buffsize, int32_t _num_words, int32_t _num_topics,
            std::string _datadir, std::vector<std::string> _extern_data_paths, std::string _internbase,
            int32_t _max_line_len){
     membudget = _membudget;
     buffsize = _buffsize;
     num_total_buffs = membudget/buffsize;
     num_total_tasks = _num_words;
     num_topics = _num_topics;
     datadir = _datadir;
     extern_data_paths = _extern_data_paths;
     internbase = _internbase;
     max_line_len = _max_line_len;
     num_tasks_per_buff = Buffer::get_data_capacity(buffsize)/Word::usize(num_topics);

     if(buffsize < KMinTaskBuffs + KMinDataBuffs + KNumExternDataBuffs) return -1;

     try{
       zmq_ctx = new zmq::context_t(1);
       diskio = new DiskIOThread(zmq_ctx, 4, KDiskIOEndp.c_str(), datadir);
       textparser = new RawTextParser(_max_line_len, "\n");
     }catch(...){
       return -1;
     }

     int suc = diskio->init();
     if(suc < 0) return -1;
     suc = diskio->start();
     if(suc < 0) return -1;

     int32_t max_task_buffs = num_total_buffs - KNumExternDataBuffs - KMinDataBuffs;
     assert(max_task_buffs > 0);

     num_total_task_buffs = (num_total_tasks + num_tasks_per_buff - 1)/num_tasks_per_buff;

     num_task_buffs = (max_task_buffs >= num_total_task_buffs) ? num_total_task_buffs : max_task_buffs;
     num_data_buffs = num_total_buffs - num_task_buffs - KNumExternDataBuffs;

     try{
       extern_data_loader = new BufferManager(KNumExternDataBuffs*buffsize, buffsize, diskio);
       task_buffer_mgr = new BufferManager(num_task_buffs*buffsize, buffsize, diskio);
       data_buffer_mgr = new BufferManager(num_data_buffs*buffsize, buffsize, diskio);
       extern_data_writer = new BufferManager(KNumExternDataBuffs*buffsize, buffsize, diskio);
     }catch(...){
       return -1;
     }

     try{
       my_task_buffs = new Buffer *[num_task_buffs];
       my_init_data_buffs = new Buffer *[num_data_buffs];
     }catch(std::bad_alloc &ba){
       std::cout << "allocate memory failed" << std::endl;
       return -1;
     }
     return 0;
   }

   int LdaExecutorInit::run(){

     int suc = init_task_buffers();
     if(suc < 0) return -1;

     suc = init_data_buffers();
     if(suc < 0) return -1;

     extern_data_loader->stop();
     delete extern_data_loader;
     extern_data_loader = NULL;

     suc = output_task_buffers();
     if(suc < 0) return -1;

     suc = output_data_buffers();
     if(suc < 0) return -1;

    return 0;
   }

   int LdaExecutorInit::cleanup(){
     diskio->stop();
     return 0;
   }
}

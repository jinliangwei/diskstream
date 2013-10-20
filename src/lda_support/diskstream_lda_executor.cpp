
#include <string.h>
#include <assert.h>
#include <sstream>
#include <iostream>
#include <fstream>
#include <limits.h>

#include "diskstream_lda_executor.hpp"
#include "../diskstream_program.hpp"

namespace diskstream {
  LdaExecutorInit::LdaExecutorInit():
       extern_data_loader(NULL),
       task_buffer_mgr(NULL),
       data_buffer_mgr(NULL),
       extern_data_writer(NULL),
       zmq_ctx(NULL),
       diskio(NULL),
       textparser(NULL),
       exemode(ExeInitRun),
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
       program(NULL),
       globdata(NULL){}

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
       delete[] my_init_data_buffs;
       my_init_data_buffs = NULL;
     }

     if(my_task_buffs != NULL){
       delete[] my_task_buffs;
       my_task_buffs = NULL;
     }
   }

   std::string LdaExecutorInit::get_status_file_name(std::string _datadir, std::string _internbase){
     return _datadir + "/" + _internbase + ".status";
   }

   int LdaExecutorInit::init_task_buffers(){
     MemChunkAccesser task_buff_acc;
     int32_t task_id = 0;
     Buffer *buffer;
     bool last_buff_putback = false;
     int suc;

     buffer = task_buffer_mgr->get_buffer();
     assert(buffer != NULL);
     if(buffer == NULL) return -1;

     int32_t task_buffs_idx = 0;
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
     return 0;
   }

   int LdaExecutorInit::init_data_buffers(){
     MemChunkAccesser data_buff_acc;
     Buffer *extern_buff, *data_buff;
     int32_t datasize, line_len;
     int suc;
     char *linebuf = new char[max_line_len];
     Document *doc;
     bool last_buff_putback = false;

     data_buff = data_buffer_mgr->get_buffer();
     assert(data_buff != NULL);
     int32_t data_buffs_idx = 0;
     data_buff_acc.set_mem(data_buff->get_dataptr(), data_buff->get_data_capacity());
     data_buff->set_block_id(data_buffs_idx);

     my_init_data_buffs[data_buffs_idx] = data_buff;
     ++data_buffs_idx;
     num_my_init_data_buffs = 1;

     assert(!extern_data_loader->reached_end());

     //int32_t total_docs_added = 0;
     while(!extern_data_loader->reached_end()){
       extern_buff = extern_data_loader->get_buffer();
       assert(extern_buff != NULL);
       datasize = extern_buff->get_valid_db_size();
       // datasize includes the ending '\0', so we need to subtract 1 from it
       suc = textparser->set_textbuf((char *) extern_buff->get_db_ptr(), datasize - 1);
       line_len = textparser->get_next(linebuf);
       //int32_t docs_added = 0;
       while(line_len > 0){
         try{
           doc = new (data_buff_acc.curr_ptr()) Document(data_buff_acc.avai_size(),
                                                         std::string(linebuf), num_topics);
           data_buff->append_data_record(data_buff_acc.curr_ptr(), doc->size());
           suc = data_buff_acc.advance(doc->size());
           assert(suc == 0);
           line_len = textparser->get_next(linebuf);
           //++docs_added;
         }catch(EDataCreateErr &e){
           //total_docs_added += docs_added;
           //std::cout << "docs added = " << docs_added
           //          << " total_docs_added = " << total_docs_added
           //          << ", then get a new buffer" << std::endl << std::endl;
           //docs_added = 0;
           if(e == DataCreateInsufficientMem){
             if(num_data_buffs <= (KMinDataBuffsMgr + num_my_init_data_buffs)){
               suc = data_buffer_mgr->putback(data_buff, BufPolicyWriteBack);
               assert(suc == 0);
               if(suc < 0) return -1;
               std::cout << "put back data_buff = " << (void *) data_buff
                         << " block_id = " << data_buff->get_block_id()
                         << " data size = " << data_buff->get_data_size()
                         << " data capacity = " << data_buff->get_data_capacity() << std::endl << std::endl;
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
     //int32_t docs_added = 0;
     if(line_len > 0){
       try{
         doc = new (data_buff_acc.curr_ptr()) Document(data_buff_acc.avai_size(),
             std::string(linebuf), num_topics);
         data_buff->append_data_record(data_buff_acc.curr_ptr(), doc->size());
         suc = data_buff_acc.advance(doc->size());
         assert(suc == 0);
       //  ++docs_added;
       }catch(EDataCreateErr &e){
       //  total_docs_added += docs_added;
       //  std::cout << "docs added = " << docs_added
       //      << " total_docs_added = " << total_docs_added
       //      << ", then get a new buffer" << std::endl << std::endl;
       //  docs_added = 0;
         if(e == DataCreateInsufficientMem){
           if(num_data_buffs <= (KMinDataBuffsMgr + num_my_init_data_buffs)){
             suc = data_buffer_mgr->putback(data_buff, BufPolicyWriteBack);
             assert(suc == 0);
             if(suc < 0) return -1;
             std::cout << " init_data_buffs put back data_buff = " << (void *) data_buff
                       << " block_id = " << data_buff->get_block_id()
                       << " data size = " << data_buff->get_data_size() << std::endl << std::endl;
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
     extern_data_loader->stop();
     return 0;
   }

   int LdaExecutorInit::output_task_buffers(std::string _extern_name){
     char *templine = new char[max_line_len];
     int suc = 0;
     int32_t tbuff_idx;
     MemChunkAccesser buff_acc, extern_buff_acc;
     extern_data_writer->init_write_extern(_extern_name);
     //extern_data_writer->init_write_extern(internbase + ".tasks.extern");
     Buffer *extern_buff = extern_data_writer->get_buffer();
     extern_buff_acc.set_mem(extern_buff->get_db_ptr(), extern_buff->get_db_size());
     bool tostop = false;
     if(num_my_task_buffs < num_total_task_buffs){
       task_buffer_mgr->init_sequen(internbase + ".task", num_my_task_buffs, 1);
       tostop = true;
     }

     int32_t num_task_buffs_accessed = 0;
     Word *w;
     do{
       for(tbuff_idx = 0; tbuff_idx < num_my_task_buffs; ++tbuff_idx){
         buff_acc.set_mem(my_task_buffs[tbuff_idx]->get_dataptr(),
                          my_task_buffs[tbuff_idx]->get_data_size());
         //std::cout << "output task_buffs, idx = " << tbuff_idx
         //          << " block_id = " << my_task_buffs[tbuff_idx] << std::endl;
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

         if(task_buffer_mgr->reached_end()){
           std::cout << "task_buffer_mgr has reached the end!!" << std::endl;
           break;
         }

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
     if(tostop)
       task_buffer_mgr->stop();
     return 0;
   }

   int LdaExecutorInit::output_data_buffers(std::string _extern_name){
     char *templine = new char[max_line_len];
     MemChunkAccesser buff_acc, extern_buff_acc;
     extern_data_writer->init_write_extern(_extern_name);
     //extern_data_writer->init_write_extern(internbase + ".data.extern");

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
           //std::cout << "strlen(templine) = " << strlen(templine) << std::endl;
           sprintf(templine + strlen(templine), " %d:%d", uwords[i], wcnts[i]);
         }
         int32_t len = strlen(templine);
         templine[len] = '\n';
         ++len;
         if(extern_buff_acc.avai_size() < len){
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

     std::cout << "num_my_init_data_buffs = " << num_my_init_data_buffs
               << " num_total_data_buffs = " << num_total_data_buffs << std::endl;

     if(num_my_init_data_buffs < num_total_data_buffs){
       data_buffer_mgr->init_sequen(internbase + ".data", num_my_init_data_buffs, 1);
       Buffer *data_buff = NULL;
       while(!data_buffer_mgr->reached_end()){
         if(data_buff != NULL){
           suc = data_buffer_mgr->putback(data_buff, BufPolicyAbort);
           assert(suc == 0);
         }
         data_buff = data_buffer_mgr->get_buffer();
         assert(data_buff != NULL);
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
             extern_buff->set_valid_db_size(extern_buff->get_db_size() - extern_buff_acc.avai_size());
             extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
             extern_buff = extern_data_writer->get_buffer();
             extern_buff_acc.set_mem(extern_buff->get_db_ptr(), extern_buff->get_db_size());
           }
           memcpy(extern_buff_acc.curr_ptr(), templine, len);
           extern_buff_acc.advance(len);
           suc = buff_acc.advance(doc->size());
           assert(suc == 0);
         }
       }
       data_buffer_mgr->stop();
     }
     extern_buff->set_valid_db_size(extern_buff->get_db_size() - extern_buff_acc.avai_size());
     extern_data_writer->putback(extern_buff, BufPolicyWriteBack);
     extern_data_writer->stop();
     return 0;
   }

   int LdaExecutorInit::save_status(){
     std::string fname = get_status_file_name(datadir, internbase);
     std::fstream ofs(fname.c_str(), std::ios_base::out);
     ofs << "BuffSize " << buffsize << "\n";
     ofs << "NumTotalTaskBuffs " << num_total_task_buffs << "\n";
     ofs << "NumTotalDataBuffs " << num_total_data_buffs << "\n";
     ofs << "NumTopics " << num_topics << "\n";
     ofs.close();
     return 0;
   }

   int LdaExecutorInit::load_status(){
     std::string fname = get_status_file_name(datadir, internbase);
     std::fstream ifs(fname.c_str(), std::ios_base::in);
     if(!ifs.good()) return -1;
     std::string s;
     ifs >> s;
     ifs >> buffsize;
     ifs >> s;
     ifs >> num_total_task_buffs;
     ifs >> s;
     ifs >> num_total_data_buffs;
     ifs.close();
     return 0;
   }

   int LdaExecutorInit::execute_one_data(uint8_t *data, int32_t _le, int32_t _ge, int32_t _niter){
     Document *doc = (Document *) data;
     int32_t num_uwords;
     const int32_t *uwords;
     uwords = doc->get_task_list(num_uwords);
     int32_t st, ed;
     int suc;
     suc = find_range(uwords, num_uwords, _le, _ge, st, ed);
     if(suc == 0){
       int32_t tidx;
       for(tidx = st; tidx < ed; ++tidx){
         uint8_t *taskptr = taskloc.get_task_ptr(uwords[tidx]);
         program->partial_reduce(taskptr, (uint8_t *) doc, globdata, _niter, tidx);
       }
     }
     return 0;
   }

   int LdaExecutorInit::execute_my_tasks(int32_t _le, int32_t _ge, int32_t _niter){
     int suc;
     MemChunkAccesser buff_acc;
     Document *doc;
     int32_t num_data_buffs_accessed = 0;

     std::cout << "execute_my_tasks(): " << "task range, le = " << _le
               << ", ge = " << _ge << std::endl;

     if(num_my_init_data_buffs > 0){
       int32_t i;
       for(i = 0; i < num_my_init_data_buffs; ++i){
         buff_acc.set_mem(my_init_data_buffs[i]->get_dataptr(),
                          my_init_data_buffs[i]->get_data_size());
         std::cout << "start executing on data buffer, i = " << i << std::endl;
         while(buff_acc.avai_size() > 0){
           doc = (Document *) buff_acc.curr_ptr();
           suc = execute_one_data((uint8_t *) doc, _le, _ge, _niter);
           suc = buff_acc.advance(doc->size());
           assert(suc == 0);
         }
         data_buffer_mgr->putback(my_init_data_buffs[i], BufPolicyWriteBack);
       }
       num_data_buffs_accessed  = num_my_init_data_buffs;
       num_my_init_data_buffs = 0;
     }

     while(num_data_buffs_accessed < num_total_data_buffs){
       Buffer *buff;
       buff = data_buffer_mgr->get_buffer();
       //std::cout << "get data buffer " << (void *) buff << std::endl;
       buff_acc.set_mem(buff->get_dataptr(), buff->get_data_size());
       std::cout << "start executing on data buffer, i = " << num_data_buffs_accessed << std::endl;
       while(buff_acc.avai_size() > 0){
         doc = (Document *) buff_acc.curr_ptr();
         suc = execute_one_data((uint8_t *) doc, _le, _ge, _niter);
         suc = buff_acc.advance(doc->size());
         assert(suc == 0);
       }
       suc = data_buffer_mgr->putback(buff, BufPolicyWriteBack);
       assert(suc == 0);
       ++num_data_buffs_accessed;
     }
     std::cout << "execute my tasks done!" << std::endl;
     return 0;
   }


   int LdaExecutorInit::execute_all_tasks(int32_t _niter){
     int32_t tidx;
     int suc;
     int32_t num_task_buffs_accessed = 0;

     std::cout << "data_buffer_mgr init_sequen done, num_my_init_data_buffs = "
               << num_my_init_data_buffs << std::endl;

     std::cout << "starting execution, num_task_buffs_accessed = " << num_task_buffs_accessed
               << " num_total_task_buffs = " << num_total_task_buffs
               << " num_my_task_buffs = " << num_my_task_buffs
               << " num_task_buffs = " << num_task_buffs << std::endl;

     int32_t le = 0, ge = -1;
     while(num_task_buffs_accessed < num_total_task_buffs){
       taskloc.reset_taskid_st(ge + 1);

       while(num_my_task_buffs < num_task_buffs
           && (num_task_buffs_accessed + num_my_task_buffs < num_total_task_buffs)){
         Buffer *taskbuff = task_buffer_mgr->get_buffer();
         std::cout << "get task buffer = " << (void *) taskbuff
                   << " num_my_task_buffs = " << num_my_task_buffs << std::endl;
         my_task_buffs[num_my_task_buffs] = taskbuff;
         ++num_my_task_buffs;
       }

       for(tidx = 0; tidx < num_my_task_buffs; ++tidx){
         suc = taskloc.add_task_buff(my_task_buffs[tidx]);
         assert(suc == 0);
       }

       std::cout << "has got task buffers, num_my_task_buffs = " << num_my_task_buffs << std::endl;
       taskloc.get_range(le, ge);
       execute_my_tasks(le, ge, _niter);

       for(tidx = 0; tidx < num_my_task_buffs; ++tidx){
         suc = task_buffer_mgr->putback(my_task_buffs[tidx], BufPolicyWriteBack);
         assert(suc == 0);
       }
       num_task_buffs_accessed += num_my_task_buffs;
       num_my_task_buffs = 0;
     }

     std::cout << "execute all tasks done!" << std::endl;
     return 0;
   }

   int LdaExecutorInit::init(int64_t _membudget, int64_t _buffsize, int32_t _num_words, int32_t _num_topics,
            std::string _datadir, std::vector<std::string> _extern_data_paths, std::string _internbase,
            int32_t _max_line_len, DiskStreamProgram *_program, uint8_t *_globdata, EExecutorMode _exemode){
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
     program = _program;
     exemode = _exemode;
     globdata = _globdata;

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

     if(exemode == ExeRun){
       load_status();
       num_total_buffs = membudget/buffsize;
     }

     int32_t max_task_buffs;
     if(num_total_buffs < (KMinDataBuffs + KMinTaskBuffs + KNumExternDataBuffs)) return -1;
     max_task_buffs = num_total_buffs - KMinDataBuffs - KNumExternDataBuffs;
     num_total_task_buffs = (num_total_tasks + num_tasks_per_buff - 1)/num_tasks_per_buff;
     num_task_buffs = std::min(max_task_buffs, num_total_task_buffs);
     num_data_buffs = num_total_buffs - num_task_buffs - KNumExternDataBuffs;

     try{
       my_task_buffs = new Buffer *[num_task_buffs];
       if((exemode == ExeInitRun || exemode == ExeInitOnly)){
         my_init_data_buffs = new Buffer *[num_data_buffs];
       }
     }catch(std::bad_alloc &ba){
       std::cout << "allocate memory failed" << std::endl;
       return -1;
     }

     try{
       task_buffer_mgr = new BufferManager(num_task_buffs*buffsize, buffsize, diskio);
       data_buffer_mgr = new BufferManager(num_data_buffs*buffsize, buffsize, diskio);
       extern_data_writer = new BufferManager(KNumExternDataBuffs*buffsize, buffsize, diskio);
     }catch(...){
       return -1;
     }

     taskloc.init(num_task_buffs, buffsize, Word::usize(num_topics));
     taskloc.reset_taskid_st(0);

     std::cout << "lda_executor creating internal objects done!" << std::endl;

     std::cout << "init done: "
               << " num_total_buffs = " << num_total_buffs
               << " num_task_buffs = " << num_task_buffs
               << " num_data_buffs = " << num_data_buffs
               << " num_total_task_buffs = " << num_total_task_buffs
               << " num_total_data_buffs = " << num_total_data_buffs
               << std::endl;

     return 0;
   }

   int LdaExecutorInit::run(int32_t _num_iters){
     int suc;
     int64_t accum_time = 0;
     int64_t last_time = timer_st();
     int64_t run_time;
     if(exemode == ExeInitRun || exemode == ExeInitOnly){
       try{
         extern_data_loader = new BufferManager(KNumExternDataBuffs*buffsize, buffsize, diskio);
       }catch(...){
         return -1;
       }

       suc = task_buffer_mgr->init(internbase + ".task");
       if(suc < 0) return -1;

       suc = extern_data_loader->init_sequen_extern(extern_data_paths);
       if(suc < 0) return -1;

       suc = data_buffer_mgr->init(internbase + ".data");
       if(suc < 0) return -1;

       std::cout << "start initializing task buffers curr_time = " << accum_time << std::endl;
       last_time = timer_st();
       suc = init_task_buffers();
       assert(suc == 0);
       if(suc < 0) return -1;

       run_time = timer_ed(last_time);
       accum_time += run_time;
       std::cout << "done initializing task buffers curr_time = " << accum_time << std::endl;

       std::cout << "start initializing data buffers curr_time = " << accum_time << std::endl;
       last_time = timer_st();

       suc = init_data_buffers();
       assert(suc == 0);

       run_time = timer_ed(last_time);
       accum_time += run_time;
       std::cout << "done initializing data buffers curr_time = " << accum_time << std::endl;

       if(suc < 0) return -1;

       save_status();

       if(exemode == ExeInitOnly){
         int32_t i;
         for(i = 0; i < num_my_init_data_buffs; ++i){
           suc = data_buffer_mgr->putback(my_init_data_buffs[i], BufPolicyWriteBack);
           assert(suc == 0);
         }

         for(i = 0; i < num_my_task_buffs; ++i){
           task_buffer_mgr->putback(my_task_buffs[i], BufPolicyWriteBack);
           assert(suc == 0);
         }

         delete extern_data_loader;
         extern_data_loader = NULL;
         data_buffer_mgr->stop();
         task_buffer_mgr->stop();
         return 0;
       }

       delete extern_data_loader;
       extern_data_loader = NULL;
       data_buffer_mgr->stop();
       task_buffer_mgr->stop();
     }

     int32_t num_task_iters = _num_iters;
     if(num_my_task_buffs == num_total_task_buffs)
       --num_task_iters;

     if(_num_iters > 0){
       suc = task_buffer_mgr->init_sequen(internbase + ".task", num_my_task_buffs, _num_iters,
                                          num_total_task_buffs);
       assert(suc == 0);
       std::cout << "task_buffer_mgr init_sequen done" << std::endl;
     }

     int32_t num_data_iters = (num_total_task_buffs + num_task_buffs - 1)/num_task_buffs;
     suc = data_buffer_mgr->init_sequen(internbase + ".data", num_my_init_data_buffs,
                                        num_data_iters*_num_iters, num_total_data_buffs);
     assert(suc == 0);

     std::cout << "start executing tasks curr_time = " << accum_time << std::endl;

     int32_t iter;
     for(iter = 0; iter < _num_iters; ++iter){
       last_time = timer_st();

       suc = execute_all_tasks(iter);
       if(suc < 0) return -1;

       run_time = timer_ed(last_time);
       accum_time += run_time;
       std::cout << "executed iteration = " << iter << " curr_time = " << accum_time << std::endl;
     }

     data_buffer_mgr->stop();
     task_buffer_mgr->stop();


     return 0;
   }

   int LdaExecutorInit::output_data(){
     std::string fname;
     if(exemode == ExeInitOnly){
       fname = internbase + ".data.extern";
     }else{
       fname = internbase + ".data.extern.after";
     }
     return output_data_buffers(fname);
   }

   int LdaExecutorInit::output_task(){
     std::string fname;
     if(exemode == ExeInitOnly){
       fname = internbase + ".task.extern";
     }else{
       fname = internbase + ".task.extern.after";
     }
     return output_task_buffers(fname);
   }

   int LdaExecutorInit::cleanup(){
     diskio->stop();
     return 0;
   }
}

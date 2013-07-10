#ifndef __DISKSTREAM_MSGTYPES_HPP__
#define __DISKSTREAM_MSGTYPES_HPP__

/**
 * General Message format
 *
 * 1. EMsgType
 * 2. Type specific information
 *
 * This format relies on 0mq's multi-part message.
 */

namespace diskstream {
  enum EMsgType {DiskIORead, DiskIOWrite, DiskIOReadDone, DiskIOWriteDone, DiskIOShutDown, DiskIOConn,
                 DiskIOReadExternal, DiskIOWriteExternal, DiskIOReadExternalDone, DiskIOWriteExternalDone,
                 DiskIOCancel, DiskIOWriteRead, DiskIOWriteReadDone, DiskIOReadNotDone, DiskIOReadExternalNotDone,
                 DiskIOWriteReadWriteDone, DiskIOEnable};

  /*
   * DiskIORead/DiskIOWrite fields:
   * 1. buffer address (Buffer *)
   * 2. base filename (char[]), must include the \0, same below
   * */

  /* DiskIOReadDone fields:
   * 1. buffer address
   * 2. read bytes (> 0 for success, negative for failure)
   * */

  /* DiskIOWriteDone fields:
   *
   * 1. buffer address
   * 2. success (0 for success, negative for failure)
   *
   * */

  /*
   * DiskIOShutDown:
   * nothing
   * */

  /*
   * DiskIOConn:
   * This is used to establish connection between sockets. DiskIOThread will ignore this message.
   * */

  /*
   * DiskIO***External is used to read/write files that are created/managed external to the DiskIO,
   * therefore, clients needs to specify full path, offset, and size to read/write
   *
   * So far, we only handles small files ( <= 2 GB)
   *
   * DiskIOReadExternal:
   * 1. buffer address (Buffer *)
   * 2. full path to file (char[])
   * 3. offset to read (int32_t)
   * 4. size to read
   *
   * DiskIOWriteExternal:
   * 1. buffer address (Buffer *)
   * 2. full path to file (char[])
   * 3. offset to write (int32_t)
   * 4. size to write
   *
   * DiskIOReadExternalDone, DiskIOWriteExternalDone:
   * 1. buffer address (Buffer *)
   * 2. read bytes/success (negative for failure)
   * */

  /*
   * DiskIOWriteRead, DiskIOWriteReadDone:
   *
   * They are used for sequential BufferManager to write back data then immediately read in another
   * data block.
   *
   * DiskIOWriteRead:
   *
   * 1. buffer address (Buffer *)
   * 2. base filename (char[]), must include the \0, same below
   * 3. Read db id
   *
   * DiskIOWriteReadDone:
   *
   * 1. buffer address (Buffer *)
   * 2. read bytes (if negative, failure)
   * */

  //const char *KDISKIO_ENDP = "inproc://diskio_endp";
}

#endif /* DISKSTREAM_MSGTYPES_HPP_ */

CPP = g++
SRCDIR = src
INCFLAGS = -I$(SRCDIR)
CPPFLAGS = -g -O2 -Wall -Wno-strict-aliasing -Wreorder -DREG_IO
LIBS = -lboost_program_options-mt -lzmq

.PHONY: all
all: lda rwtest pnew ds_lda

.PHONY: test
test: unit_buffer

test_lda : tests/lda/lda.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) tests/lda/lda.cpp $(LIBS) -o bin/test_lda
	
rwtest : tests/read_write_test/rwtest.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) tests/read_write_test/rwtest.cpp $(LIBS) -o bin/rwtest
	
pnew : tests/placement_new.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) tests/placement_new.cpp -o bin/pnew
	
unit_buffer: tests/unit_buffer.cpp src/diskstream_types.hpp src/diskstream_buffer.cpp src/diskstream_buffer.hpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) $(INCFLAGS) tests/unit_buffer.cpp src/diskstream_buffer.cpp $(LIBS) \
	-o bin/unit_buffer
	
unit_iothread: tests/unit_iothread.cpp src/diskstream_buffer.hpp src/diskstream_buffer.cpp \
			   src/diskstream_iothread.hpp src/diskstream_iothread.cpp src/diskstream_types.hpp \
			   src/diskstream_msgtypes.hpp src/util/diskstream_util.hpp src/util/diskstream_util.cpp \
			   src/util/diskstream_zmq_util.hpp src/util/diskstream_zmq_util.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) $(INCFLAGS) tests/unit_iothread.cpp src/diskstream_buffer.cpp \
	src/diskstream_iothread.cpp src/util/diskstream_util.cpp src/util/diskstream_zmq_util.cpp \
	$(LIBS) -o bin/unit_iothread
	
unit_allfilessize: tests/unit_allfilessize.cpp src/diskstream_buffer.hpp src/diskstream_buffer.cpp \
			   src/diskstream_iothread.hpp src/diskstream_iothread.cpp src/diskstream_types.hpp \
			   src/diskstream_msgtypes.hpp src/util/diskstream_util.hpp src/util/diskstream_util.cpp \
			   src/util/diskstream_zmq_util.hpp src/util/diskstream_zmq_util.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) $(INCFLAGS) tests/unit_allfilessize.cpp src/diskstream_buffer.cpp \
	src/diskstream_iothread.cpp src/util/diskstream_util.cpp src/util/diskstream_zmq_util.cpp \
	$(LIBS) -o bin/unit_allfilessize
	
ds_emplda : tests/diskstream_lda/lda.cpp src/diskstream_buffer.hpp src/diskstream_buffer.cpp \
         src/diskstream_iothread.hpp src/diskstream_iothread.cpp src/diskstream_types.hpp \
		 src/diskstream_msgtypes.hpp src/util/diskstream_util.hpp src/util/diskstream_util.cpp \
	     src/util/diskstream_zmq_util.hpp src/util/diskstream_zmq_util.cpp \
	     src/diskstream_buffermanager.hpp src/diskstream_buffermanager.cpp \
	     src/lda_support/diskstream_lda_executor.cpp src/lda_support/diskstream_lda_executor.hpp \
	     src/lda_support/lda_datastruct.hpp src/lda_support/lda_datastruct.cpp src/diskstream_program.hpp \
	     src/diskstream_program.cpp src/diskstream_tasklocator.hpp src/diskstream_tasklocator.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) $(INCFLAGS) tests/diskstream_lda/lda.cpp src/diskstream_buffer.cpp \
	src/diskstream_iothread.cpp src/util/diskstream_util.cpp src/util/diskstream_zmq_util.cpp \
	src/lda_support/diskstream_lda_executor.cpp src/diskstream_buffermanager.cpp src/lda_support/lda_datastruct.cpp \
	src/diskstream_program.cpp src/diskstream_tasklocator.cpp \
	$(LIBS) -o bin/ds_emplda

ldagen : tests/data_generator.cpp src/diskstream_buffer.hpp src/diskstream_buffer.cpp \
         src/diskstream_iothread.hpp src/diskstream_iothread.cpp src/diskstream_types.hpp \
		 src/diskstream_msgtypes.hpp src/util/diskstream_util.hpp src/util/diskstream_util.cpp \
	     src/util/diskstream_zmq_util.hpp src/util/diskstream_zmq_util.cpp \
	     src/diskstream_buffermanager.hpp src/diskstream_buffermanager.cpp \
	     src/data_generator/diskstream_lda_datagen.hpp src/data_generator/diskstream_lda_datagen.cpp 
	mkdir -p bin
	$(CPP) $(CPPFLAGS) $(INCFLAGS) tests/data_generator.cpp src/diskstream_buffer.cpp \
	src/diskstream_iothread.cpp src/util/diskstream_util.cpp src/util/diskstream_zmq_util.cpp \
	src/diskstream_buffermanager.cpp src/data_generator/diskstream_lda_datagen.cpp \
	$(LIBS) -o bin/ldagen

	
unit_findrange : tests/unit_findrange.cpp src/util/diskstream_util.hpp src/util/diskstream_util.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) $(INCFLAGS) tests/unit_findrange.cpp src/util/diskstream_util.cpp $(LIBS) -o bin/unit_findgrange
	
clean :
	rm -rf bin
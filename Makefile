CPP = g++
SRCDIR = src
INCFLAGS = -I$(SRCDIR)
CPPFLAGS = -g -O2 -Wall -Wno-strict-aliasing
LIBS = -lboost_program_options-mt -lzmq

.PHONY: all
all: lda rwtest pnew

.PHONY: test
test: unit_buffer

lda : apps/lda/lda.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) apps/lda/lda.cpp $(LIBS) -o bin/lda
	
rwtest : apps/read_write_test/rwtest.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) apps/read_write_test/rwtest.cpp $(LIBS) -o bin/rwtest
	
pnew : apps/placement_new.cpp
	mkdir -p bin
	$(CPP) $(CPPFLAGS) apps/placement_new.cpp -o bin/pnew
	
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
clean :
	rm -rf bin
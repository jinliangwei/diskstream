
#include <iostream>
#include <stdint.h>

namespace test{
  struct Test{
    int a;
    int b;

    Test(uint32_t size){
      std::cout << "this = " << (void *) this << std::endl;
      a = 1;
      b = 2;
      if(size < sizeof(Test)){
        throw new std::bad_alloc;
      }
    }
  };
  Test *make_test(void *mem){
    return new (mem) Test(10);
  }
}
using namespace test;
const int d = sizeof(Test)*2 + 1;

int main(int argc, char *argv[]){
  int8_t *mem = new int8_t[10];
  Test *t = make_test(mem);

  std::cout << " d = " << d << std::endl;
  std::cout << "mem = " << (void *) mem << std::endl;
  std::cout << "t = " << (void *) t << std::endl;
  delete[] mem;
  return 0;
}

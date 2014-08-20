#include <iostream>
#include <memory.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

void dout_emergency(const char * const str)
{
  std::cerr << str;
  std::cerr.flush();
}

void dout_emergency(const std::string &str)
{
  std::cerr << str;
  std::cerr.flush();
}


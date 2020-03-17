#include "example.h"
#include <execinfo.h>
#include <ray/api.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <thread>

using namespace ray::api;

void handler(int sig) {
  void *array[10];
  size_t size;
  size = backtrace(array, 10);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

int Foo0() { return 1; }
int Foo(int x) { return x + 1; }

int Bar(int x, int y) { return x + y; }

int main() {
  signal(SIGSEGV, handler);
  signal(SIGTERM, handler);
  signal(SIGINT, handler);
  signal(SIGILL, handler);
  signal(SIGABRT, handler);
  signal(SIGFPE, handler);
  Ray::Init();

  auto obj = Ray::Put(123);
  auto getRsult = obj.Get();

  auto r0 = Ray::Call(Foo0);
  auto r1 = Ray::Call(Foo, 1);
  auto r2 = Ray::Call(Bar, 1, 2);
  auto r3 = Ray::Call(FooTest::Foo_s, 3);
  auto r4 = Ray::Call(FooTest::Bar_s, 3, 2);

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());
  int result3 = *(r3.Get());
  int result4 = *(r4.Get());

  std::cout << "Ray::call with value results: " << result0 << " " << result1 << " "
            << result2 << " " << result3 << " " << result4 << " " << std::endl;

  auto rt0 = Ray::Call(Foo0);
  auto rt1 = Ray::Call(Foo, rt0);
  auto rt2 = Ray::Call(Bar, rt1, 1);
  auto rt3 = Ray::Call(FooTest::Foo_s, 3);
  auto rt4 = Ray::Call(FooTest::Bar_s, rt0, rt3);

  int return0 = *(rt0.Get());
  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());

  std::cout << "Ray::call with reference results: " << return0 << " " << return1 << " "
            << return2 << " " << return3 << " " << return4 << " " << std::endl;

  RayActor<FooTest> actor = Ray::CreateActor(FooTest::Create);
  auto rt5 = actor.Call(&FooTest::Foo, 1);
  auto rt6 = actor.Call(&FooTest::Bar, 1, rt5);
  auto rt7 = actor.Call(&FooTest::Add, 4);
  auto rt8 = actor.Call(&FooTest::Add, 1);
  auto rt9 = actor.Call(&FooTest::Add, 1);
  auto rt10 = actor.Call(&FooTest::Add, rt9);

  int return5 = *(rt5.Get());
  int return6 = *(rt6.Get());
  int return7 = *(rt7.Get());
  int return8 = *(rt8.Get());
  int return9 = *(rt9.Get());
  int return10 = *(rt10.Get());

  std::cout << "Ray::call with actor results: " << return5 << " " << return6 << " "
            << return7 << " " << return8 << " " << return9 << " " << return10 << " "
            << std::endl;
}

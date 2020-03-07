

// TODO: code generation

// 0 args
template <typename R>
using Func0 = R (*)();

// 1 args
template <typename R, typename T1>
using Func1 = R (*)(T1);

// 2 args
template <typename R, typename T1, typename T2>
using Func2 = R (*)(T1, T2);

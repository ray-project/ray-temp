#include "invocation_executor.h"
#include "../../agent.h"
#include "../abstract_ray_runtime.h"

namespace ray { namespace api {

void InvocationExecutor::execute(const LocalTaskSpec &taskSpec,
                                 std::shared_ptr<msgpack::sbuffer> actor) {
  if (actor) {
    typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
        uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
        std::shared_ptr<msgpack::sbuffer> object);
    EXEC_FUNCTION exec_function =
        (EXEC_FUNCTION)(dylib_base_addr + taskSpec.get_exec_func_offset());
    auto data = (*exec_function)(dylib_base_addr, taskSpec.get_func_offset(),
                                 taskSpec.args, actor);
    AbstractRayRuntime &rayRuntime = AbstractRayRuntime::getInstance();
    rayRuntime.put(std::move(data), taskSpec.returnId, taskSpec.taskId);
  } else {
    typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
        uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
    EXEC_FUNCTION exec_function =
        (EXEC_FUNCTION)(dylib_base_addr + taskSpec.get_exec_func_offset());
    auto data =
        (*exec_function)(dylib_base_addr, taskSpec.get_func_offset(), taskSpec.args);
    AbstractRayRuntime &rayRuntime = AbstractRayRuntime::getInstance();
    rayRuntime.put(std::move(data), taskSpec.returnId, taskSpec.taskId);
  }
}
}  }// namespace ray::api
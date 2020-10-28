import ray
import os
import signal
import time
import sys


def test_was_current_actor_reconstructed(shutdown_only):
    ray.init()

    @ray.remote(max_restarts=10)
    class A(object):
        def __init__(self):
            self._was_reconstructed = ray.get_runtime_context(
            ).was_current_actor_reconstructed

        def get_was_reconstructed(self):
            return self._was_reconstructed

        def update_was_reconstructed(self):
            return ray.get_runtime_context().was_current_actor_reconstructed

        def get_pid(self):
            return os.getpid()

        # The following methods is to apply the checkpointable interface.
        def should_checkpoint(self, checkpoint_context):
            return False

        def save_checkpoint(self, actor_id, checkpoint_id):
            pass

        def load_checkpoint(self, actor_id, available_checkpoints):
            pass

        def checkpoint_expired(self, actor_id, checkpoint_id):
            pass

    a = A.remote()
    # `was_reconstructed` should be False when it's called in actor.
    assert ray.get(a.get_was_reconstructed.remote()) is False
    # `was_reconstructed` should be False when it's called in a remote method
    # and the actor never fails.
    assert ray.get(a.update_was_reconstructed.remote()) is False

    pid = ray.get(a.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    time.sleep(2)
    # These 2 methods should be return True because
    # this actor failed and restored.
    assert ray.get(a.get_was_reconstructed.remote()) is True
    assert ray.get(a.update_was_reconstructed.remote()) is True

    @ray.remote(max_restarts=10)
    class A(object):
        def current_job_id(self):
            return ray.get_runtime_context().job_id

        def current_actor_id(self):
            return ray.get_runtime_context().actor_id

    @ray.remote
    def f():
        assert ray.get_runtime_context().actor_id is None
        assert ray.get_runtime_context().task_id is not None
        assert ray.get_runtime_context().node_id is not None
        assert ray.get_runtime_context().job_id is not None
        context = ray.get_runtime_context().get()
        assert "actor_id" not in context
        assert context["task_id"] == ray.get_runtime_context().task_id
        assert context["node_id"] == ray.get_runtime_context().node_id
        assert context["job_id"] == ray.get_runtime_context().job_id

    a = A.remote()
    assert ray.get(a.current_job_id.remote()) is not None
    assert ray.get(a.current_actor_id.remote()) is not None
    ray.get(f.remote())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

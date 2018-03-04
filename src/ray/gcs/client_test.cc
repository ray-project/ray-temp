#include "gtest/gtest.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"

namespace ray {

aeEventLoop *loop;

class TestGcs : public ::testing::Test {
 public:
  TestGcs() {
    RAY_CHECK_OK(client_.Connect("127.0.0.1", 6379));
    job_id_ = UniqueID::from_random();
  }

 protected:
  gcs::AsyncGcsClient client_;
  UniqueID job_id_;
};

void ObjectAdded(gcs::AsyncGcsClient *client,
                 const UniqueID &id,
                 std::shared_ptr<ObjectTableDataT> data) {
  ASSERT_EQ(data->managers, std::vector<std::string>({"A", "B"}));
}

void Lookup(gcs::AsyncGcsClient *client,
            const UniqueID &id,
            std::shared_ptr<ObjectTableDataT> data) {
  // Check that the object entry was added.
  ASSERT_EQ(data->managers, std::vector<std::string>({"A", "B"}));
  aeStop(loop);
}

TEST_F(TestGcs, TestObjectTable) {
  loop = aeCreateEventLoop(1024);
  RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop));
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  data->managers.push_back("B");
  ObjectID object_id = ObjectID::from_random();
  RAY_CHECK_OK(
      client_.object_table().Add(job_id_, object_id, data, &ObjectAdded));
  RAY_CHECK_OK(client_.object_table().Lookup(job_id_, object_id, &Lookup));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  aeMain(loop);
  aeDeleteEventLoop(loop);
}

void TaskAdded(gcs::AsyncGcsClient *client,
               const TaskID &id,
               std::shared_ptr<TaskTableDataT> data) {
  ASSERT_EQ(data->scheduling_state, SchedulingState_SCHEDULED);
}

void TaskLookup(gcs::AsyncGcsClient *client,
                const TaskID &id,
                std::shared_ptr<TaskTableDataT> data) {
  ASSERT_EQ(data->scheduling_state, SchedulingState_SCHEDULED);
}

void TaskLookupAfterUpdate(gcs::AsyncGcsClient *client,
                           const TaskID &id,
                           std::shared_ptr<TaskTableDataT> data) {
  ASSERT_EQ(data->scheduling_state, SchedulingState_LOST);
  aeStop(loop);
}

void TaskUpdateCallback(gcs::AsyncGcsClient *client,
                        const TaskID &task_id,
                        const TaskTableDataT &task,
                        bool updated) {
  RAY_CHECK_OK(client->task_table().Lookup(DriverID::nil(), task_id,
                                           &TaskLookupAfterUpdate));
}

TEST_F(TestGcs, TestTaskTable) {
  loop = aeCreateEventLoop(1024);
  RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop));
  auto data = std::make_shared<TaskTableDataT>();
  data->scheduling_state = SchedulingState_SCHEDULED;
  DBClientID local_scheduler_id =
      DBClientID::from_binary("abcdefghijklmnopqrst");
  data->scheduler_id = local_scheduler_id.binary();
  TaskID task_id = TaskID::from_random();
  RAY_CHECK_OK(client_.task_table().Add(job_id_, task_id, data, &TaskAdded));
  RAY_CHECK_OK(client_.task_table().Lookup(job_id_, task_id, &TaskLookup));
  auto update = std::make_shared<TaskTableTestAndUpdateT>();
  update->test_scheduler_id = local_scheduler_id.binary();
  update->test_state_bitmask = SchedulingState_SCHEDULED;
  update->update_state = SchedulingState_LOST;
  // After test-and-setting, the callback will lookup the current state of the
  // task.
  RAY_CHECK_OK(client_.task_table().TestAndUpdate(job_id_, task_id, update,
                                                  &TaskUpdateCallback));
  // Run the event loop. The loop will only stop if the lookup after the
  // test-and-set succeeds (or an assertion failure).
  aeMain(loop);
  aeDeleteEventLoop(loop);
}

void objectTableSubscribed(gcs::AsyncGcsClient *client,
                           const UniqueID &id,
                           std::shared_ptr<ObjectTableDataT> data) {
  aeStop(loop);
}

TEST_F(TestGcs, TestSubscribeAll) {
  loop = aeCreateEventLoop(1024);
  RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop));
  // Subscribe to all object table notifications. The registered callback for
  // notifications will check whether the object below is added.
  RAY_CHECK_OK(client_.object_table().Subscribe(
      job_id_, ClientID::nil(), &Lookup, &objectTableSubscribed));
  // Run the event loop. The loop will only stop if the subscription succeeds.
  aeMain(loop);

  // We have subscribed. Add an object table entry.
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  data->managers.push_back("B");
  ObjectID object_id = ObjectID::from_random();
  RAY_CHECK_OK(
      client_.object_table().Add(job_id_, object_id, data, &ObjectAdded));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  aeMain(loop);
  aeDeleteEventLoop(loop);
}

}  // namespace

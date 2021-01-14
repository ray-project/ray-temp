
#include "ray/object_manager/pull_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/common_protocol.h"
#include "ray/common/test_util.h"

namespace ray {

using ::testing::ElementsAre;

class PullManagerTestWithCapacity {
 public:
  PullManagerTestWithCapacity(size_t num_available_bytes)
      : self_node_id_(NodeID::FromRandom()),
        object_is_local_(false),
        num_send_pull_request_calls_(0),
        num_restore_spilled_object_calls_(0),
        fake_time_(0),
        pull_manager_(
            self_node_id_, [this](const ObjectID &object_id) { return object_is_local_; },
            [this](const ObjectID &object_id, const NodeID &node_id) {
              num_send_pull_request_calls_++;
            },
            [this](const ObjectID &, const std::string &,
                   std::function<void(const ray::Status &)> callback) {
              num_restore_spilled_object_calls_++;
              restore_object_callback_ = callback;
            },
            [this]() { return fake_time_; }, 10000, num_available_bytes) {}

  // TODO: Check no memory leaks.

  NodeID self_node_id_;
  bool object_is_local_;
  int num_send_pull_request_calls_;
  int num_restore_spilled_object_calls_;
  std::function<void(const ray::Status &)> restore_object_callback_;
  double fake_time_;
  PullManager pull_manager_;
};

class PullManagerTest : public PullManagerTestWithCapacity, public ::testing::Test {
 public:
  PullManagerTest() : PullManagerTestWithCapacity(1) {}

  void AssertNumActiveRequestsEquals(size_t num_requests) {
    ASSERT_EQ(pull_manager_.object_pull_requests_.size(), num_requests);
    ASSERT_EQ(pull_manager_.active_object_pull_requests_.size(), num_requests);
  }
};

class PullManagerWithAdmissionControlTest : public PullManagerTestWithCapacity,
                                            public ::testing::Test {
 public:
  PullManagerWithAdmissionControlTest() : PullManagerTestWithCapacity(10) {}

  void AssertNumActiveRequestsEquals(size_t num_requests) {
    ASSERT_EQ(pull_manager_.active_object_pull_requests_.size(), num_requests);
  }

  bool IsUnderCapacity(size_t num_bytes_requested) {
    return num_bytes_requested <= pull_manager_.num_bytes_available_;
  }
};

std::vector<rpc::ObjectReference> CreateObjectRefs(int num_objs) {
  std::vector<rpc::ObjectReference> refs;
  for (int i = 0; i < num_objs; i++) {
    ObjectID obj = ObjectID::FromRandom();
    rpc::ObjectReference ref;
    ref.set_object_id(obj.Binary());
    refs.push_back(ref);
  }
  return refs;
}

TEST_F(PullManagerTest, TestStaleSubscription) {
  auto refs = CreateObjectRefs(1);
  auto oid = ObjectRefsToIds(refs)[0];
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(oid, client_ids, "", 0);

  // There are no client ids to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));

  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(0);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(oid, client_ids, "", 0);

  // Now we're getting a notification about an object that was already cancelled.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(0);
}

TEST_F(PullManagerTest, TestRestoreSpilledObject) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  client_ids.insert(NodeID::FromRandom());
  fake_time_ += 10.;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);

  // The behavior is supposed to be to always restore the spilled object if possible (even
  // if it exists elsewhere in the cluster).
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  // Don't restore an object if it's local.
  object_is_local_ = true;
  num_restore_spilled_object_calls_ = 0;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(0);
}

TEST_F(PullManagerTest, TestRestoreObjectFailed) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  restore_object_callback_(ray::Status::IOError(":("));

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);

  // We always assume the restore succeeded so there's only 1 restore call still.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  fake_time_ += 10.0;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);

  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  restore_object_callback_(ray::Status::IOError(":("));

  // Since restore failed, we can fallback to pulling from another node immediately.
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar", 0);

  // Now that we've successfully sent a pull request, we need to wait for the retry period
  // before sending another one.
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);
}

TEST_F(PullManagerTest, TestManyUpdates) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  for (int i = 0; i < 100; i++) {
    pull_manager_.OnLocationChange(obj1, client_ids, "", 0);
  }

  // Since no time has passed, only send a single pull request.
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(0);
}

TEST_F(PullManagerTest, TestRetryTimer) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  // We need to call OnLocationChange at least once, to population the list of nodes with
  // the object.
  pull_manager_.OnLocationChange(obj1, client_ids, "", 0);
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  for (; fake_time_ <= 7 * 10; fake_time_ += 1.) {
    pull_manager_.Tick();
  }

  // Location changes can trigger reset timer.
  for (; fake_time_ <= 120 * 10; fake_time_ += 1.) {
    pull_manager_.OnLocationChange(obj1, client_ids, "", 0);
  }

  // We should make a pull request every tick (even if it's a duplicate to a node we're
  // already pulling from).
  ASSERT_EQ(num_send_pull_request_calls_, 7);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // Don't retry an object if it's local.
  object_is_local_ = true;
  num_send_pull_request_calls_ = 0;
  for (; fake_time_ <= 127 * 10; fake_time_ += 1.) {
    pull_manager_.Tick();
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  AssertNumActiveRequestsEquals(0);
}

TEST_F(PullManagerTest, TestBasic) {
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  AssertNumActiveRequestsEquals(oids.size());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", 0);
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }

  // Don't pull an object if it's local.
  object_is_local_ = true;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, oids);
  AssertNumActiveRequestsEquals(0);

  // Don't pull a remote object if we've canceled.
  object_is_local_ = false;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);
}

TEST_F(PullManagerTest, TestDeduplicateBundles) {
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id1 = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  AssertNumActiveRequestsEquals(oids.size());

  objects_to_locate.clear();
  auto req_id2 = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_TRUE(objects_to_locate.empty());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", 0);
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }

  // Cancel one request.
  auto objects_to_cancel = pull_manager_.CancelPull(req_id1);
  ASSERT_TRUE(objects_to_cancel.empty());
  // Objects should still be pulled because the other request is still open.
  AssertNumActiveRequestsEquals(oids.size());
  fake_time_ += 10;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", 0);
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }

  // Cancel the other request.
  objects_to_cancel = pull_manager_.CancelPull(req_id2);
  ASSERT_EQ(objects_to_cancel, oids);
  AssertNumActiveRequestsEquals(0);

  // Don't pull a remote object if we've canceled.
  object_is_local_ = false;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);
}

TEST_F(PullManagerWithAdmissionControlTest, TestBasic) {
  /// Test admission control for a single pull bundle request. We should
  /// activate the request when we are under the reported capacity and
  /// deactivate it when we are over.
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  size_t object_size = 2;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  AssertNumActiveRequestsEquals(oids.size());
  ASSERT_TRUE(IsUnderCapacity(oids.size() * object_size));

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", object_size);
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }
  AssertNumActiveRequestsEquals(oids.size());
  ASSERT_TRUE(IsUnderCapacity(oids.size() * object_size));

  // Reduce the available memory.
  pull_manager_.UpdatePullsBasedOnAvailableMemory(oids.size() * object_size - 1);
  AssertNumActiveRequestsEquals(0);
  // No new pull requests after the next tick.
  fake_time_ += 10;
  auto prev_pull_requests = num_send_pull_request_calls_;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", object_size);
    ASSERT_EQ(num_send_pull_request_calls_, prev_pull_requests);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }

  // Increase the available memory again.
  pull_manager_.UpdatePullsBasedOnAvailableMemory(oids.size() * object_size);
  AssertNumActiveRequestsEquals(oids.size());
  ASSERT_TRUE(IsUnderCapacity(oids.size() * object_size));
  // Pull requests should get triggered at the next tick.
  ASSERT_EQ(num_send_pull_request_calls_, prev_pull_requests);
  pull_manager_.Tick();
  ASSERT_EQ(num_send_pull_request_calls_, prev_pull_requests + oids.size());

  pull_manager_.CancelPull(req_id);
  AssertNumActiveRequestsEquals(0);
}

TEST_F(PullManagerWithAdmissionControlTest, TestQueue) {
  /// Test admission control for a queue of pull bundle requests. We should
  /// activate as many requests as we can, subject to the reported capacity.
  int object_size = 2;
  int num_oids_per_request = 2;
  int num_requests = 3;

  std::vector<std::vector<ObjectID>> bundles;
  std::vector<int64_t> req_ids;
  for (int i = 0; i < num_requests; i++) {
    auto refs = CreateObjectRefs(num_oids_per_request);
    auto oids = ObjectRefsToIds(refs);
    std::vector<rpc::ObjectReference> objects_to_locate;
    auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
    ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);

    bundles.push_back(oids);
    req_ids.push_back(req_id);
  }
  AssertNumActiveRequestsEquals(num_oids_per_request * num_requests);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (auto &oids : bundles) {
    for (size_t i = 0; i < oids.size(); i++) {
      pull_manager_.OnLocationChange(oids[i], client_ids, "", object_size);
    }
  }

  for (int capacity = 0; capacity < 20; capacity++) {
    int num_requests_expected =
        std::min(num_requests, capacity / (object_size * num_oids_per_request));
    pull_manager_.UpdatePullsBasedOnAvailableMemory(capacity);

    AssertNumActiveRequestsEquals(num_requests_expected * num_oids_per_request);
    // The total requests that are active is under the specified capacity.
    ASSERT_TRUE(
        IsUnderCapacity(num_requests_expected * num_oids_per_request * object_size));
    // This is the maximum number of requests that can be served at once that
    // is under the capacity.
    if (num_requests_expected < num_requests) {
      ASSERT_FALSE(IsUnderCapacity((num_requests_expected + 1) * num_oids_per_request *
                                   object_size));
    }
  }
}

TEST_F(PullManagerWithAdmissionControlTest, TestCancel) {
  /// Test admission control while requests are cancelled out-of-order. When an
  /// active request is cancelled, we should activate another request in the
  /// queue, if there is one that satisfies the reported capacity.
  int object_size = 2;
  int num_oids_per_request = 2;
  int num_requests = 6;

  std::vector<std::vector<ObjectID>> bundles;
  std::vector<int64_t> req_ids;
  for (int i = 0; i < num_requests; i++) {
    auto refs = CreateObjectRefs(num_oids_per_request);
    auto oids = ObjectRefsToIds(refs);
    std::vector<rpc::ObjectReference> objects_to_locate;
    auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
    ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);

    bundles.push_back(oids);
    req_ids.push_back(req_id);
  }
  AssertNumActiveRequestsEquals(num_oids_per_request * num_requests);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (auto &oids : bundles) {
    for (size_t i = 0; i < oids.size(); i++) {
      pull_manager_.OnLocationChange(oids[i], client_ids, "", object_size);
    }
  }

  // We have enough capacity for half of the requests at a time.
  int capacity = object_size * num_oids_per_request * num_requests / 2;
  int num_requests_expected = num_requests / 2;
  pull_manager_.UpdatePullsBasedOnAvailableMemory(capacity);
  AssertNumActiveRequestsEquals(num_requests_expected * num_oids_per_request);

  // Cancel the last request that is being served.
  pull_manager_.CancelPull(req_ids[2]);
  req_ids.erase(req_ids.begin() + 2);
  AssertNumActiveRequestsEquals(num_requests_expected * num_oids_per_request);

  // Cancel the middle request that is being served.
  pull_manager_.CancelPull(req_ids[1]);
  req_ids.erase(req_ids.begin() + 1);
  AssertNumActiveRequestsEquals(num_requests_expected * num_oids_per_request);

  // Cancel the head request that is being served.
  pull_manager_.CancelPull(req_ids[0]);
  req_ids.erase(req_ids.begin());
  AssertNumActiveRequestsEquals(num_requests_expected * num_oids_per_request);

  while (!req_ids.empty()) {
    pull_manager_.CancelPull(req_ids[0]);
    req_ids.erase(req_ids.begin());
    AssertNumActiveRequestsEquals(req_ids.size() * num_oids_per_request);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class MockPlacementGroupScheduler : public gcs::GcsPlacementGroupSchedulerInterface {
 public:
  MockPlacementGroupScheduler() = default;

  void ScheduleUnplacedBundles(
      std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> failure_handler,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> success_handler)
      override {
    placement_groups_.push_back(placement_group);
  }

  MOCK_METHOD1(DestroyPlacementGroupBundleResourcesIfExists,
               void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(MarkScheduleCancelled, void(const PlacementGroupID &placement_group_id));

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) override {
    absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles;
    bundles[group_on_dead_node_] = bundles_on_dead_node_;
    return bundles;
  }

  PlacementGroupID group_on_dead_node_;
  std::vector<int64_t> bundles_on_dead_node_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> placement_groups_;
};

class GcsPlacementGroupManagerTest : public ::testing::Test {
 public:
  GcsPlacementGroupManagerTest()
      : mock_placement_group_scheduler_(new MockPlacementGroupScheduler()) {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, io_service_, gcs_pub_sub_, gcs_table_storage_);
    gcs_placement_group_manager_.reset(
        new gcs::GcsPlacementGroupManager(io_service_, mock_placement_group_scheduler_,
                                          gcs_table_storage_, *gcs_node_manager_));
  }

  void SetUp() override {
    // mock_placement_group_scheduler_.reset(new MockPlacementGroupScheduler());
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  std::shared_ptr<MockPlacementGroupScheduler> mock_placement_group_scheduler_;
  std::unique_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;

 private:
  std::unique_ptr<std::thread> thread_io_service_;
  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(GcsPlacementGroupManagerTest, TestBasic) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(finished_placement_group_count, 0);

  // Check that the placement_group is in state `CREATED`.
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name"),
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id()));
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeAdd) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If the creation of placement group fails, it will be rescheduled after a short time.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  auto condition = [this]() {
    return (int)mock_placement_group_scheduler_->placement_groups_.size() == 1;
  };
  EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingPendingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  std::atomic<int> failed_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count, &failed_placement_group_count](Status status) {
        if (status.ok()) {
          ++finished_placement_group_count;
        } else {
          ++failed_placement_group_count;
        }
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(failed_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](Status status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();
  WaitForExpectedCount(finished_placement_group_count, 0);
  WaitForExpectedCount(failed_placement_group_count, 1);

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](Status status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingLeasingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  std::atomic<int> failed_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count, &failed_placement_group_count](Status status) {
        if (status.ok()) {
          ++finished_placement_group_count;
        } else {
          ++failed_placement_group_count;
        }
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(failed_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);

  // Placement group is in leasing state.
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](Status status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();
  WaitForExpectedCount(finished_placement_group_count, 0);
  WaitForExpectedCount(failed_placement_group_count, 1);

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](Status status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingCreatedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        if (status.ok()) {
          ++finished_placement_group_count;
        }
      });
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](Status status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(finished_placement_group_count, 1);

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](Status status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeDead) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request1),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  auto request2 = Mocker::GenCreatePlacementGroupRequest();
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request2),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  placement_group->GetMutableBundle(0)->set_node_id(NodeID::FromRandom().Binary());
  placement_group->GetMutableBundle(1)->set_node_id(NodeID::FromRandom().Binary());
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If a node dies, we will set the bundles above it to be unplaced and reschedule the
  // placement group. The placement group state is set to `RESCHEDULING` and will be
  // scheduled first.
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());

  // Trigger scheduling `RESCHEDULING` placement group.
  auto finished_group = std::make_shared<gcs::GcsPlacementGroup>(
      placement_group->GetPlacementGroupTableData());
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(finished_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
  const auto &bundles =
      mock_placement_group_scheduler_->placement_groups_[0]->GetBundles();
  EXPECT_TRUE(NodeID::FromBinary(bundles[0]->GetMutableMessage().node_id()).IsNil());
  EXPECT_FALSE(NodeID::FromBinary(bundles[1]->GetMutableMessage().node_id()).IsNil());

  // If `RESCHEDULING` placement group fails to create, we will schedule it again first.
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  auto condition = [this]() {
    return (int)mock_placement_group_scheduler_->placement_groups_.size() == 1;
  };
  EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
}

TEST_F(GcsPlacementGroupManagerTest, TestRegisteredPlacementGroupIndex) {
  // Test the case where the placement group has only a job id.
  gcs::RegisteredPlacementGroupIndex registered_placement_groups;
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ JobID::FromInt(1),
      /* actor_id */ ActorID::Nil());
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(gcs::GcsPlacementGroup(request));
  auto job_id = placement_group->GetCreatorJobId();
  // Make sure when we emplace twice, the second function won't do anything and return
  // false.
  ASSERT_TRUE(registered_placement_groups.Emplace(placement_group->GetPlacementGroupID(),
                                                  placement_group));
  ASSERT_FALSE(registered_placement_groups.Emplace(placement_group->GetPlacementGroupID(),
                                                   placement_group));
  // Make sure we can get entries that exist.
  ASSERT_TRUE(registered_placement_groups.GetPlacementGroupsOwnedByJob(job_id).size() ==
              1);
  ASSERT_TRUE(placement_group ==
              registered_placement_groups.Get(placement_group->GetPlacementGroupID()));
  ASSERT_TRUE(registered_placement_groups
                  .GetPlacementGroupsOwnedByActor(ActorID::Of(job_id, TaskID::Nil(), 0))
                  .size() == 0);
  // Make sure we cannot get entries that don'e exist.
  ASSERT_TRUE(registered_placement_groups.GetPlacementGroupsOwnedByJob(JobID::FromInt(2))
                  .size() == 0);
  ASSERT_TRUE(registered_placement_groups
                  .GetPlacementGroupsOwnedByActor(
                      ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0))
                  .size() == 0);
  ASSERT_FALSE(registered_placement_groups.Get(PlacementGroupID::FromRandom()) ==
               placement_group);
  // Make sure things are cleaned after erasing them.
  registered_placement_groups.Erase(placement_group->GetPlacementGroupID());
  ASSERT_TRUE(registered_placement_groups.GetPlacementGroupsOwnedByJob(job_id).size() ==
              0);
  ASSERT_FALSE(registered_placement_groups.Get(placement_group->GetPlacementGroupID()) ==
               placement_group);
  // Make sure we can emplace the same entry again.
  ASSERT_TRUE(registered_placement_groups.Emplace(placement_group->GetPlacementGroupID(),
                                                  placement_group));
  registered_placement_groups.Erase(placement_group->GetPlacementGroupID());

  // Test the case where placement group has both actor id and job id.
  auto job_id2 = JobID::FromInt(2);
  auto actor_id = ActorID::Of(job_id2, TaskID::Nil(), 0);
  auto request2 = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id2,
      /* actor_id */ actor_id);
  auto placement_group2 =
      std::make_shared<gcs::GcsPlacementGroup>(gcs::GcsPlacementGroup(request2));
  ASSERT_TRUE(registered_placement_groups.Emplace(placement_group2->GetPlacementGroupID(),
                                                  placement_group2));
  ASSERT_TRUE(registered_placement_groups.GetPlacementGroupsOwnedByJob(job_id2).size() ==
              1);
  ASSERT_TRUE(placement_group2 ==
              registered_placement_groups.Get(placement_group2->GetPlacementGroupID()));
  ASSERT_TRUE(
      registered_placement_groups.GetPlacementGroupsOwnedByActor(actor_id).size() == 1);
  registered_placement_groups.Erase(placement_group2->GetPlacementGroupID());
  ASSERT_TRUE(registered_placement_groups.GetPlacementGroupsOwnedByJob(job_id2).size() ==
              0);
  ASSERT_FALSE(placement_group2 ==
               registered_placement_groups.Get(placement_group2->GetPlacementGroupID()));
  ASSERT_TRUE(
      registered_placement_groups.GetPlacementGroupsOwnedByActor(actor_id).size() == 0);

  // Register 2 placement groups with the same job id.
  auto request3 = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id2,
      /* actor_id */ ActorID::Nil());
  auto placement_group3 =
      std::make_shared<gcs::GcsPlacementGroup>(gcs::GcsPlacementGroup(request3));
  ASSERT_TRUE(registered_placement_groups.Emplace(placement_group2->GetPlacementGroupID(),
                                                  placement_group2));
  ASSERT_TRUE(registered_placement_groups.Emplace(placement_group3->GetPlacementGroupID(),
                                                  placement_group3));
  ASSERT_TRUE(registered_placement_groups.GetPlacementGroupsOwnedByJob(job_id2).size() ==
              2);
  ASSERT_TRUE(
      registered_placement_groups.GetPlacementGroupsOwnedByActor(actor_id).size() == 1);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenActorDeadAndJobDead) {
  // Test the scenario where actor dead -> job dead.
  auto job_id = JobID::FromInt(1);
  auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenActorAndJobDead) {
  // Test the scenario where job dead -> actor dead.
  auto job_id = JobID::FromInt(1);
  auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenOnlyJobDead) {
  // Test placement group is cleaned when both actor & job are dead.
  auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest,
       TestAutomaticCleanupDoNothingWhenDifferentJobIsDead) {
  // Test placement group is cleaned when both actor & job are dead.
  auto job_id = JobID::FromInt(1);
  auto different_job_id = JobID::FromInt(3);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  // This shouldn't have been called.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

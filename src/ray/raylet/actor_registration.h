#ifndef RAY_RAYLET_ACTOR_REGISTRATION_H
#define RAY_RAYLET_ACTOR_REGISTRATION_H

#include <unordered_map>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

class ActorRegistration {
 public:
  ActorRegistration(const ActorTableDataT &actor_table_data);

  const ClientID GetNodeManagerId() const;
  const ObjectID GetActorCreationDependency() const;

  void ExtendFrontier(const ActorHandleID &handle_id,
                      const ObjectID &execution_dependency);

 private:
  struct FrontierLeaf {
    int64_t task_counter;
    ObjectID execution_dependency;
  };

  ActorTableDataT actor_table_data_;
  ObjectID execution_dependency_;
  std::unordered_map<ActorHandleID, FrontierLeaf, UniqueIDHasher> frontier_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_ACTOR_REGISTRATION_H

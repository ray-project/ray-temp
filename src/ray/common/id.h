#ifndef RAY_ID_H_
#define RAY_ID_H_

#include <inttypes.h>
#include <limits.h>

#include <chrono>
#include <cstring>
#include <mutex>
#include <random>
#include <string>

#include "plasma/common.h"
#include "ray/common/constants.h"
#include "ray/util/logging.h"
#include "ray/util/visibility.h"

namespace ray {

class WorkerID;
class UniqueID;
class JobID;

/// TODO(qwang): These 2 helper functions should be removed
/// once we separated the `WorkerID` from `UniqueID`.
///
/// A helper function that get the `DriverID` of the given job.
WorkerID ComputeDriverIdFromJob(const JobID &job_id);

enum class ObjectType : uint8_t {
  PUT_OBJECT,
  RETURN_OBJECT,
};

enum class TransportType : uint8_t {
  STANDARD,
  DIRECT_ACTOR_CALL,
};

namespace object_id_helper {

constexpr uint8_t is_task_offset_bits = 15;
constexpr uint8_t object_type_offset_bits = 14;
constexpr uint8_t transport_type_offset_bits = 11;

/// A helper function to set object ids flags.
inline void SetIsTaskFlag(uint16_t *flags, bool is_task) {
  uint16_t is_task_bits;
  if (is_task) {
    is_task_bits = (0x1 << is_task_offset_bits);
  } else {
    is_task_bits = (0x0 << is_task_offset_bits);
  }
  *flags = (*flags bitor is_task_bits);
}

inline bool IsTask(uint16_t flags) {
  return (flags & (0x1 << is_task_offset_bits)) != 0;
}

inline ObjectType GetObjectType(uint16_t flags) {
  uint16_t object_type = ((flags >> object_type_offset_bits) & 0x1);
  if (object_type == 0x0) {
    return ObjectType::PUT_OBJECT;
  } else if (object_type == 0x1) {
    return ObjectType::RETURN_OBJECT;
  } else {
    RAY_LOG(FATAL) << "Shouldn't reach here.";
  }
}

inline TransportType GetTransportType(uint16_t flags) {
  uint16_t type = ((flags >> transport_type_offset_bits) & 0x7);

  if (type == 0x0) {
    return TransportType::STANDARD;
  } else if (type == 0x1) {
    return TransportType::DIRECT_ACTOR_CALL;
  } else {
    RAY_LOG(FATAL) << "Shouldn't reach here.";
  }
}

inline void SetObjectTypeFlag(uint16_t *flags, ObjectType object_type) {
  uint16_t object_type_bits;
  if (object_type == ObjectType::PUT_OBJECT) {
    object_type_bits = (0x0 << object_type_offset_bits);
  } else if (object_type == ObjectType::RETURN_OBJECT) {
    object_type_bits = (0x1 << object_type_offset_bits);
  } else {
    RAY_LOG(FATAL) << "Shouldn't be reachable here.";
  }
  *flags = (*flags bitor object_type_bits);
}

inline void SetTransportTypeFlag(uint16_t *flags, TransportType transport_type) {
  uint16_t transport_type_bits;
  if (transport_type == TransportType::STANDARD) {
    transport_type_bits = (0x0 << transport_type_offset_bits);
  } else if (transport_type == TransportType::DIRECT_ACTOR_CALL) {
    transport_type_bits = (0x1 << transport_type_offset_bits);
  } else {
    RAY_LOG(FATAL) << "Shouldn't be reachable here.";
  }
  *flags = (*flags bitor transport_type_bits);
}

} // namespace object_id_helper

// Declaration.
std::mt19937 RandomlySeededMersenneTwister();
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

template <typename T>
class BaseID {
 public:
  BaseID();
  static T FromRandom();
  static T FromBinary(const std::string &binary);
  static const T &Nil();
  static size_t Size() { return T::Size(); }

  size_t Hash() const;
  bool IsNil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *Data() const;
  std::string Binary() const;
  std::string Hex() const;

 protected:
  BaseID(const std::string &binary) {
    std::memcpy(const_cast<uint8_t *>(this->Data()), binary.data(), T::Size());
  }
  // All IDs are immutable for hash evaluations. MutableData is only allow to use
  // in construction time, so this function is protected.
  uint8_t *MutableData();
  // For lazy evaluation, be careful to have one Id contained in another.
  // This hash code will be duplicated.
  mutable size_t hash_ = 0;
};

class UniqueID : public BaseID<UniqueID> {
 public:
  static size_t Size() { return kUniqueIDSize; }

  UniqueID() : BaseID() {}

 protected:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
};

class JobID : public BaseID<JobID> {
 public:
  static constexpr int64_t length = 4;

  static JobID FromInt(uint32_t value);

  static size_t Size() { return length; }

  static JobID FromRandom() = delete;

  JobID() : BaseID() {}

 private:
  uint8_t id_[length];
};

class ActorID : public BaseID<ActorID> {
 private:
  static constexpr size_t unique_bytes_length = 4;

 public:
  static constexpr size_t length = unique_bytes_length + JobID::length;

  static size_t Size() { return length; }

  static ActorID FromRandom(const JobID &job_id);

  ActorID() : BaseID() {}

  JobID JobId() const;

  static ActorID FromRandom() = delete;

 private:
  uint8_t id_[length];
};

class TaskID : public BaseID<TaskID> {
 private:
  static constexpr size_t unique_bytes_length = 6;

 public:
  static constexpr size_t length = unique_bytes_length + ActorID::length;

  TaskID() : BaseID() {}

  static size_t Size() { return length; }

  static TaskID ComputeDriverTaskId(const WorkerID &driver_id);

  static TaskID FromRandom(const ActorID &actor_id);

 private:
  uint8_t id_[length];
};

// TODO(qwang): Add complete designing to describe structure of ID.
class ObjectID : public BaseID<ObjectID> {
private:
  static constexpr size_t unique_bytes_length = 4;

  static constexpr size_t flags_bytes_length = 2;

 public:
  static constexpr size_t length = unique_bytes_length + flags_bytes_length + TaskID::length;

  ObjectID() : BaseID() {}

  static size_t Size() { return length; }

  static ObjectID FromPlasmaIdBinary(const std::string &from);

  plasma::ObjectID ToPlasmaId() const;

  ObjectID(const plasma::UniqueID &from);

  /// Get the index of this object in the task that created it.
  ///
  /// \return The index of object creation according to the task that created
  /// this object. This is positive if the task returned the object and negative
  /// if created by a put.
  uint32_t ObjectIndex() const;

  /// Compute the task ID of the task that created the object.
  ///
  /// \return The task ID of the task that created this object.
  TaskID TaskId() const;

  bool IsTask() const {
    uint16_t flags;
    std::memcpy(&flags, id_ + TaskID::length, sizeof(flags));
    return object_id_helper::IsTask(flags);
  }

  bool IsPutObject() const {
    uint16_t flags;
    std::memcpy(&flags, id_ + TaskID::length, sizeof(flags));
    return object_id_helper::GetObjectType(flags) == ObjectType::PUT_OBJECT;
  }

  bool IsReturnObject() const {
    uint16_t flags;
    std::memcpy(&flags, id_ + TaskID::length, sizeof(flags));
    return object_id_helper::GetObjectType(flags) == ObjectType::RETURN_OBJECT;
  }

  TransportType GetTransportType() const {
    uint16_t flags;
    std::memcpy(&flags, id_ + TaskID::length, sizeof(flags));
    return object_id_helper::GetTransportType(flags);
  }

  /// Compute the object ID of an object put by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index What index of the object put in the task.
  ///
  /// \return The computed object ID.
  static ObjectID ForPut(const TaskID &task_id, uint32_t put_index);

  /// Compute the object ID of an object returned by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param return_index What index of the object returned by in the task.
  /// \param
  ///
  /// \return The computed object ID.
  static ObjectID ForTaskReturn(const TaskID &task_id, uint32_t return_index,
                                TransportType transport = TransportType::STANDARD);

  // TODO(qwang): Add get Flags methods.

  /// \param transport
  ///
  /// \return
  static ObjectID FromRandom(TransportType transport = TransportType::STANDARD);

 private:
  uint8_t id_[length];
};

static_assert(sizeof(JobID) == JobID::length + sizeof(size_t),
              "JobID size is not as expected");
static_assert(sizeof(ActorID) == ActorID::length + sizeof(size_t),
              "ActorID size is not as expected");
static_assert(sizeof(TaskID) == TaskID::length + sizeof(size_t),
              "TaskID size is not as expected");
//static_assert(sizeof(ObjectID) == sizeof(int32_t) + sizeof(TaskID),
//              "ObjectID size is not as expected");
static_assert(sizeof(ObjectID) == ObjectID::length + sizeof(size_t),
              "ObjectID size is not as expected");

std::ostream &operator<<(std::ostream &os, const UniqueID &id);
std::ostream &operator<<(std::ostream &os, const JobID &id);
std::ostream &operator<<(std::ostream &os, const ActorID &id);
std::ostream &operator<<(std::ostream &os, const TaskID &id);
std::ostream &operator<<(std::ostream &os, const ObjectID &id);

#define DEFINE_UNIQUE_ID(type)                                                 \
  class RAY_EXPORT type : public UniqueID {                                    \
   public:                                                                     \
    explicit type(const UniqueID &from) {                                      \
      std::memcpy(&id_, from.Data(), kUniqueIDSize);                           \
    }                                                                          \
    type() : UniqueID() {}                                                     \
    static type FromRandom() { return type(UniqueID::FromRandom()); }          \
    static type FromBinary(const std::string &binary) { return type(binary); } \
    static type Nil() { return type(UniqueID::Nil()); }                        \
    static size_t Size() { return kUniqueIDSize; }                             \
                                                                               \
   private:                                                                    \
    explicit type(const std::string &binary) {                                 \
      std::memcpy(&id_, binary.data(), kUniqueIDSize);                         \
    }                                                                          \
  };

#include "id_def.h"

#undef DEFINE_UNIQUE_ID

// Restore the compiler alignment to defult (8 bytes).
#pragma pack(pop)

/// Generate a task ID from the given info.
///
/// \param job_id The job that creates the task.
/// \param parent_task_id The parent task of this task.
/// \param parent_task_counter The task index of the worker.
/// \return The task ID generated from the given info.
const TaskID GenerateTaskId(const JobID &job_id, const TaskID &parent_task_id,
                            int parent_task_counter);

/// Compute the next actor handle ID of a new actor handle during a fork operation.
///
/// \param actor_handle_id The actor handle ID of original actor.
/// \param num_forks The count of forks of original actor.
/// \return The next actor handle ID generated from the given info.
const ActorHandleID ComputeNextActorHandleId(const ActorHandleID &actor_handle_id,
                                             int64_t num_forks);

template <typename T>
BaseID<T>::BaseID() {
  // Using const_cast to directly change data is dangerous. The cached
  // hash may not be changed. This is used in construction time.
  std::fill_n(this->MutableData(), T::Size(), 0xff);
}

template <typename T>
T BaseID<T>::FromRandom() {
  std::string data(T::Size(), 0);
  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = RandomlySeededMersenneTwister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < T::Size(); i++) {
    data[i] = static_cast<uint8_t>(dist(generator));
  }
  return T::FromBinary(data);
}

template <typename T>
T BaseID<T>::FromBinary(const std::string &binary) {
  RAY_CHECK(binary.size() == T::Size()) << "expected size is "
                                        << T::Size() << ", but got " << binary.size();
  T t = T::Nil();
  std::memcpy(t.MutableData(), binary.data(), T::Size());
  return t;
}

template <typename T>
const T &BaseID<T>::Nil() {
  static const T nil_id;
  return nil_id;
}

template <typename T>
bool BaseID<T>::IsNil() const {
  static T nil_id = T::Nil();
  return *this == nil_id;
}

template <typename T>
size_t BaseID<T>::Hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(Data(), T::Size(), 0);
  }
  return hash_;
}

template <typename T>
bool BaseID<T>::operator==(const BaseID &rhs) const {
  return std::memcmp(Data(), rhs.Data(), T::Size()) == 0;
}

template <typename T>
bool BaseID<T>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template <typename T>
uint8_t *BaseID<T>::MutableData() {
  return reinterpret_cast<uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
const uint8_t *BaseID<T>::Data() const {
  return reinterpret_cast<const uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
std::string BaseID<T>::Binary() const {
  return std::string(reinterpret_cast<const char *>(Data()), T::Size());
}

template <typename T>
std::string BaseID<T>::Hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = Data();
  std::string result;
  for (int i = 0; i < T::Size(); i++) {
    unsigned int val = id[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

}  // namespace ray

namespace std {

#define DEFINE_UNIQUE_ID(type)                                           \
  template <>                                                            \
  struct hash<::ray::type> {                                             \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };                                                                     \
  template <>                                                            \
  struct hash<const ::ray::type> {                                       \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };

DEFINE_UNIQUE_ID(UniqueID);
DEFINE_UNIQUE_ID(JobID);
DEFINE_UNIQUE_ID(ActorID);
DEFINE_UNIQUE_ID(TaskID);
DEFINE_UNIQUE_ID(ObjectID);
#include "id_def.h"

#undef DEFINE_UNIQUE_ID
}  // namespace std
#endif  // RAY_ID_H_

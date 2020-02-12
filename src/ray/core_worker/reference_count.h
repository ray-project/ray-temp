#ifndef RAY_CORE_WORKER_REF_COUNT_H
#define RAY_CORE_WORKER_REF_COUNT_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/logging.h"

#include <boost/bind.hpp>

namespace ray {

/// Class used by the core worker to keep track of ObjectID reference counts for garbage
/// collection. This class is thread safe.
class ReferenceCounter {
 public:
  using ReferenceTableProto =
      ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>;

  ReferenceCounter(bool distributed_ref_counting_enabled = true,
                   rpc::ClientFactoryFn client_factory = nullptr)
      : distributed_ref_counting_enabled_(distributed_ref_counting_enabled),
        client_factory_(client_factory) {}

  ~ReferenceCounter() {}

  /// Increase the reference count for the ObjectID by one. If there is no
  /// entry for the ObjectID, one will be created. The object ID will not have
  /// any owner information, since we don't know how it was created.
  ///
  /// \param[in] object_id The object to to increment the count for.
  void AddLocalReference(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Decrease the local reference count for the ObjectID by one.
  ///
  /// \param[in] object_id The object to decrement the count for.
  /// \param[out] deleted List to store objects that hit zero ref count.
  void RemoveLocalReference(const ObjectID &object_id, std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Add references for the provided object IDs that correspond to them being
  /// dependencies to a submitted task.
  ///
  /// \param[in] object_ids The object IDs to add references for.
  void AddSubmittedTaskReferences(const std::vector<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Remove references for the provided object IDs that correspond to them being
  /// dependencies to a submitted task. This should be called when inlined
  /// dependencies are inlined or when the task finishes for plasma dependencies.
  ///
  /// \param[in] object_ids The object IDs to remove references for.
  /// \param[in] worker_addr The address of the worker that executed the task.
  /// \param[in] borrowed_refs The references that the worker borrowed during
  /// the task. Some references in this table may still be borrowed by the
  /// worker and/or a task that the worker submitted.
  /// \param[out] deleted The object IDs whos reference counts reached zero.
  void RemoveSubmittedTaskReferences(const std::vector<ObjectID> &object_ids,
                                     const rpc::Address &worker_addr,
                                     const ReferenceTableProto &borrowed_refs,
                                     std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Add an object that we own. The object may depend on other objects.
  /// Dependencies for each ObjectID must be set at most once. The local
  /// reference count for the ObjectID is set to zero, which assumes that an
  /// ObjectID for it will be created in the language frontend after this call.
  ///
  /// TODO(swang): We could avoid copying the owner_id and owner_address since
  /// we are the owner, but it is easier to store a copy for now, since the
  /// owner ID will change for workers executing normal tasks and it is
  /// possible to have leftover references after a task has finished.
  ///
  /// \param[in] object_id The ID of the object that we own.
  /// \param[in] owner_id The ID of the object's owner.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[in] dependencies The objects that the object depends on.
  void AddOwnedObject(const ObjectID &object_id, const TaskID &owner_id,
                      const rpc::Address &owner_address) LOCKS_EXCLUDED(mutex_);

  /// Add an object that we are borrowing.
  ///
  /// \param[in] object_id The ID of the object that we are borrowing.
  /// \param[in] outer_id The ID of the object that contained this object ID,
  /// if one exists. An outer_id may not exist if object_id was inlined
  /// directly in a task spec, or if it was passed in the application
  /// out-of-band.
  /// \param[in] owner_id The ID of the owner of the object. This is either the
  /// task ID (for non-actors) or the actor ID of the owner.
  /// \param[in] owner_address The owner's address.
  bool AddBorrowedObject(const ObjectID &object_id, const ObjectID &outer_id,
                         const TaskID &owner_id, const rpc::Address &owner_address)
      LOCKS_EXCLUDED(mutex_);

  /// Get the owner ID and address of the given object.
  ///
  /// \param[in] object_id The ID of the object to look up.
  /// \param[out] owner_id The TaskID of the object owner.
  /// \param[out] owner_address The address of the object owner.
  bool GetOwner(const ObjectID &object_id, TaskID *owner_id,
                rpc::Address *owner_address) const LOCKS_EXCLUDED(mutex_);

  /// Manually delete the objects from the reference counter.
  void DeleteReferences(const std::vector<ObjectID> &object_ids) LOCKS_EXCLUDED(mutex_);

  /// Sets the callback that will be run when the object goes out of scope.
  /// Returns true if the object was in scope and the callback was added, else false.
  bool SetDeleteCallback(const ObjectID &object_id,
                         const std::function<void(const ObjectID &)> callback)
      LOCKS_EXCLUDED(mutex_);

  /// Returns the total number of ObjectIDs currently in scope.
  size_t NumObjectIDsInScope() const LOCKS_EXCLUDED(mutex_);

  /// Returns whether this object has an active reference.
  bool HasReference(const ObjectID &object_id) const LOCKS_EXCLUDED(mutex_);

  /// Returns a set of all ObjectIDs currently in scope (i.e., nonzero reference count).
  std::unordered_set<ObjectID> GetAllInScopeObjectIDs() const LOCKS_EXCLUDED(mutex_);

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const
      LOCKS_EXCLUDED(mutex_);

  /// Populate a table with ObjectIDs that we were or are still borrowing.
  /// This should be called when a task returns, and the argument should be any
  /// IDs that were serialized in the task spec.
  ///
  /// See GetAndStripBorrowedRefsInternal for the spec of the returned table
  /// and how this mutates the local reference count.
  ///
  /// \param[in] borrowed_ids The object IDs that we were or are still
  /// borrowing. These are the IDs that were given to us via task submission
  /// and includes: (1) any IDs that were inlined in the task spec, and (2) any
  /// IDs that the task's arguments contained.
  /// \param[out] proto The protobuf table to populate with the borrowed
  /// references.
  void GetAndStripBorrowedRefs(const std::vector<ObjectID> &borrowed_ids,
                               ReferenceTableProto *proto) LOCKS_EXCLUDED(mutex_);

  /// Wrap an ObjectID(s) inside another object ID.
  ///
  /// \param[in] object_id The object ID whose value we are storing.
  /// \param[in] inner_ids The object IDs that we are storing in object_id.
  /// \param[in] owner_address An optional owner address for the outer
  /// object_id. If this is not provided, then we must be the owner.
  void WrapObjectId(const ObjectID &object_id, const std::vector<ObjectID> &inner_ids,
                    const absl::optional<rpc::WorkerAddress> &owner_address)
      LOCKS_EXCLUDED(mutex_);

  /// Handler for when a borrower's ref count goes to 0. This is called by the
  /// owner of the object ID. The borrower will respond when its RefCount() for
  /// the object ID goes to 0.
  void HandleWaitForRefRemoved(const rpc::WaitForRefRemovedRequest &request,
                               rpc::WaitForRefRemovedReply *reply,
                               rpc::SendReplyCallback send_reply_callback)
      LOCKS_EXCLUDED(mutex_);

  /// Whether we have a reference to a particular ObjectID.
  ///
  /// \param[in] object_id The object ID to check for.
  /// \return Whether we have a reference to the object ID.
  bool HasReference(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

 private:
  struct Reference {
    /// Constructor for a reference whose origin is unknown.
    Reference() : owned_by_us(false) {}
    /// Constructor for a reference that we created.
    Reference(const TaskID &owner_id, const rpc::Address &owner_address)
        : owned_by_us(true), owner({owner_id, owner_address}) {}

    /// Constructor from a protobuf. This is assumed to be a message from
    /// another process, so the object defaults to not being owned by us.
    static Reference FromProto(const rpc::ObjectReferenceCount &ref_count);
    /// Serialize to a protobuf.
    void ToProto(rpc::ObjectReferenceCount *ref) const;

    /// The reference count. This number includes:
    /// - Python references to the ObjectID.
    /// - Pending submitted tasks that depend on the object.
    /// - ObjectIDs that we own, that contain this ObjectID, and that are still
    ///   in scope.
    size_t RefCount() const {
      return local_ref_count + submitted_task_ref_count + contained_in_owned.size();
    }

    /// Whether we can delete this reference. A reference can NOT be deleted if
    /// any of the following are true:
    /// - The reference is still being used by this process.
    /// - The reference was contained in another ID that we were borrowing, and
    ///   we haven't told the process that gave us that ID yet.
    /// - We gave the reference to another process(es).
    bool CanDelete() const {
      bool in_scope = RefCount() > 0;
      bool was_contained_in_borrowed_id = contained_in_borrowed_id.has_value();
      bool has_borrowers = borrowers.size() > 0;
      return !(in_scope || was_contained_in_borrowed_id || has_borrowers);
    }

    /// Whether we own the object. If we own the object, then we are
    /// responsible for tracking the state of the task that creates the object
    /// (see task_manager.h).
    bool owned_by_us;
    /// The object's owner, if we know it. This has no value if the object is
    /// if we do not know the object's owner (because distributed ref counting
    /// is not yet implemented).
    absl::optional<std::pair<TaskID, rpc::Address>> owner;

    /// The local ref count for the ObjectID in the language frontend.
    size_t local_ref_count = 0;
    /// The ref count for submitted tasks that depend on the ObjectID.
    size_t submitted_task_ref_count = 0;
    /// Object IDs that we own and that contain this object ID.
    /// ObjectIDs are added to this field when we discover that this object
    /// contains other IDs. This can happen in 2 cases:
    ///  1. We call ray.put() and store the inner ID(s) in the outer object.
    ///  2. A task that we submitted returned an ID(s).
    /// ObjectIDs are erased from this field when their Reference is deleted.
    absl::flat_hash_set<ObjectID> contained_in_owned;
    /// An Object ID that we (or one of our children) borrowed that contains
    /// this object ID, which is also borrowed. This is used in cases where an
    /// ObjectID is nested. We need to notify the owner of the outer ID of any
    /// borrowers of this object, so we keep this field around until
    /// GetAndStripBorrowedRefsInternal is called on the outer ID. This field
    /// is updated in 2 cases:
    ///  1. We deserialize an ID that we do not own and that was stored in
    ///     another object that we do not own.
    ///  2. Case (1) occurred for a task that we submitted and we also do not
    ///     own the inner or outer object. Then, we need to notify our caller
    ///     that the task we submitted is a borrower for the inner ID.
    /// This field is reset to null once GetAndStripBorrowedRefsInternal is
    /// called on contained_in_borrowed_id.
    absl::optional<ObjectID> contained_in_borrowed_id;
    /// The object IDs contained in this object. These could be objects that we
    /// own or are borrowing. This field is updated in 2 cases:
    ///  1. We call ray.put() on this ID and store the contained IDs.
    ///  2. We call ray.get() on an ID whose contents we do not know and we
    ///     discover that it contains these IDs.
    absl::flat_hash_set<ObjectID> contains;
    /// A list of processes that are we gave a reference to that are still
    /// borrowing the ID. This field is updated in 2 cases:
    ///  1. If we are a borrower of the ID, then we add a process to this list
    ///     if we passed that process a copy of the ID via task submission and
    ///     the process is still using the ID by the time it finishes its task.
    ///     Borrowers are removed from the list when we recursively merge our
    ///     list into the owner.
    ///  2. If we are the owner of the ID, then either the above case, or when
    ///     we hear from a borrower that it has passed the ID to other
    ///     borrowers. A borrower is removed from the list when it responds
    ///     that it is no longer using the reference.
    absl::flat_hash_set<rpc::WorkerAddress> borrowers;

    /// Callback that will be called when this ObjectID no longer has
    /// references.
    std::function<void(const ObjectID &)> on_delete;
    /// Callback that is called when this process is no longer a borrower
    /// (RefCount() == 0).
    std::function<void()> on_local_ref_deleted;
  };

  using ReferenceTable = absl::flat_hash_map<ObjectID, Reference>;

  /// Deserialize a ReferenceTable.
  static ReferenceTable ReferenceTableFromProto(const ReferenceTableProto &proto);

  /// Serialize a ReferenceTable.
  static void ReferenceTableToProto(const ReferenceTable &table,
                                    ReferenceTableProto *proto);

  /// Populates the table with the ObjectID that we were or are still
  /// borrowing. The table also includes any IDs that we discovered were
  /// contained in the ID. For each borrowed ID, we will return:
  /// - The borrowed ID's owner's address.
  /// - Whether we are still using the ID or not (RefCount() > 0).
  /// - Addresses of new borrowers that we passed the ID to.
  /// - Whether the borrowed ID was contained in another ID that we borrowed.
  ///
  /// We will also attempt to strip the information put into the returned table
  /// that we no longer need in our local table. Each reference in the local
  /// table is modified in the following way:
  /// - For each borrowed ID, remove the addresses of any new borrowers.
  /// - For each ID that was contained in a borrowed ID, forget that the ID
  ///   that contained it.
  bool GetAndStripBorrowedRefsInternal(const ObjectID &object_id,
                                       ReferenceTable *borrower_refs)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Merge a worker's borrowed refs, and recursively all refs that they
  /// contain, into our own ref counts. This is the converse of
  /// GetAndStripBorrowedRefs. For each ID borrowed by the worker, we will:
  /// - Add the worker to our list of borrowers if it is still using the
  ///   reference.
  /// - Add the worker's accumulated borrowers to our list of borrowers.
  /// - If the borrowed ID was nested in another borrowed ID, then mark it as
  ///   such so that we can later merge the inner ID's reference into its
  ///   owner.
  /// - If we are the owner of the ID, then also contact any new borrowers and
  ///   wait for them to stop using the reference.
  void MergeBorrowedRefs(const ObjectID &object_id, const rpc::WorkerAddress &worker_addr,
                         const ReferenceTable &borrowed_refs)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Wait for a borrower to stop using its reference. This should only be
  /// called by the owner of the ID.
  /// \param[in] reference_it Iterator pointing to the reference that we own.
  /// \param[in] addr The address of the borrower.
  /// \param[in] contained_in_id Whether the owned ID was contained in another
  /// ID. This is used in cases where we return an object ID that we own inside
  /// an object that we do not own. Then, we must notify the owner of the outer
  /// object that they are borrowing the inner.
  void WaitForRefRemoved(const ReferenceTable::iterator &reference_it,
                         const rpc::WorkerAddress &addr,
                         const ObjectID &contained_in_id = ObjectID::Nil())
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to add an object that we are borrowing. This is used when
  /// deserializing IDs from a task's arguments, or when deserializing an ID
  /// during ray.get().
  bool AddBorrowedObjectInternal(const ObjectID &object_id, const ObjectID &outer_id,
                                 const TaskID &owner_id,
                                 const rpc::Address &owner_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Respond to the object's owner once we are no longer borrowing it.
  void OnRefRemoved(const ObjectID &object_id, rpc::WaitForRefRemovedReply *reply,
                    rpc::SendReplyCallback send_reply_callback)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to delete an entry from the reference map and run any necessary
  /// callbacks. Assumes that the entry is in object_id_refs_ and invalidates the
  /// iterator.
  void DeleteReferenceInternal(ReferenceTable::iterator entry,
                               std::vector<ObjectID> *deleted)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Feature flag for distributed ref counting. If this is false, then we will
  /// keep the distributed ref count, but only the local ref count will be used
  /// to decide when objects can be evicted.
  bool distributed_ref_counting_enabled_;

  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;

  /// Map from worker address to core worker client. The owner of an object
  /// uses this client to request a notification from borrowers once the
  /// borrower's ref count for the ID goes to 0.
  absl::flat_hash_map<rpc::WorkerAddress, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      borrower_cache_ GUARDED_BY(mutex_);

  /// Protects access to the reference counting state.
  mutable absl::Mutex mutex_;

  /// Holds all reference counts and dependency information for tracked ObjectIDs.
  ReferenceTable object_id_refs_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_REF_COUNT_H

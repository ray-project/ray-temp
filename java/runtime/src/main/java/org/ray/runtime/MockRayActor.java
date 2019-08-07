package org.ray.runtime;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicReference;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.Language;

public class MockRayActor extends AbstractRayActor implements Externalizable {

  private ActorId actorId;

  private AtomicReference<ObjectId> previousActorTaskDummyObjectId = new AtomicReference<>();

  public MockRayActor(ActorId actorId, ObjectId previousActorTaskDummyObjectId) {
    this.actorId = actorId;
    this.previousActorTaskDummyObjectId.set(previousActorTaskDummyObjectId);
  }

  /**
   * Required by FST
   */
  public MockRayActor() {
  }

  @Override
  public ActorId getId() {
    return actorId;
  }

  @Override
  public UniqueId getHandleId() {
    return UniqueId.NIL;
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }

  public ObjectId exchangePreviousActorTaskDummyObjectId(ObjectId previousActorTaskDummyObjectId) {
    return this.previousActorTaskDummyObjectId.getAndSet(previousActorTaskDummyObjectId);
  }

  @Override
  public synchronized void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(actorId);
    out.writeObject(previousActorTaskDummyObjectId.get());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = (ActorId) in.readObject();
    previousActorTaskDummyObjectId.set((ObjectId) in.readObject());
  }
}

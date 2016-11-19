import os
import random
import socket
import subprocess
import time
import libplasma

PLASMA_ID_SIZE = 20
PLASMA_WAIT_TIMEOUT = 2 ** 36

class PlasmaBuffer(object):
  """This is the type of objects returned by calls to get with a PlasmaClient.

  We define our own class instead of directly returning a buffer object so that
  we can add a custom destructor which notifies Plasma that the object is no
  longer being used, so the memory in the Plasma store backing the object can
  potentially be freed.

  Attributes:
    buffer (buffer): A buffer containing an object in the Plasma store.
    plasma_id (PlasmaID): The ID of the object in the buffer.
    plasma_client (PlasmaClient): The PlasmaClient that we use to communicate
      with the store and manager.
  """
  def __init__(self, buff, plasma_id, plasma_client):
    """Initialize a PlasmaBuffer."""
    self.buffer = buff
    self.plasma_id = plasma_id
    self.plasma_client = plasma_client

  def __del__(self):
    """Notify Plasma that the object is no longer needed.

    If the plasma client has been shut down, then don't do anything.
    """
    if self.plasma_client.alive:
      libplasma.release(self.plasma_client.conn, self.plasma_id)

  def __getitem__(self, index):
    """Read from the PlasmaBuffer as if it were just a regular buffer."""
    return self.buffer[index]

  def __setitem__(self, index, value):
    """Write to the PlasmaBuffer as if it were just a regular buffer.

    This should fail because the buffer should be read only.
    """
    self.buffer[index] = value

  def __len__(self):
    """Return the length of the buffer."""
    return len(self.buffer)

class PlasmaClient(object):
  """The PlasmaClient is used to interface with a plasma store and a plasma manager.

  The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a
  buffer, and get a buffer. Buffers are referred to by object IDs, which are
  strings.
  """

  def __init__(self, store_socket_name, manager_socket_name=None, release_delay=64):
    """Initialize the PlasmaClient.

    Args:
      store_socket_name (str): Name of the socket the plasma store is listening at.
      manager_socket_name (str): Name of the socket the plasma manager is listening at.
    """
    self.alive = True

    if manager_socket_name is not None:
      self.conn = libplasma.connect(store_socket_name, manager_socket_name, release_delay)
    else:
      self.conn = libplasma.connect(store_socket_name, "", release_delay)

  def shutdown(self):
    """Shutdown the client so that it does not send messages.

    If we kill the Plasma store and Plasma manager that this client is connected
    to, then we can use this method to prevent the client from trying to send
    messages to the killed processes.
    """
    if self.alive:
      libplasma.disconnect(self.conn)
    self.alive = False

  def create(self, object_id, size, metadata=None):
    """Create a new buffer in the PlasmaStore for a particular object ID.

    The returned buffer is mutable until seal is called.

    Args:
      object_id (str): A string used to identify an object.
      size (int): The size in bytes of the created buffer.
      metadata (buffer): An optional buffer encoding whatever metadata the user
        wishes to encode.
    """
    # Turn the metadata into the right type.
    metadata = bytearray("") if metadata is None else metadata
    buff = libplasma.create(self.conn, object_id, size, metadata)
    return PlasmaBuffer(buff, object_id, self)

  def get(self, object_id):
    """Create a buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block. The retrieved
    buffer is immutable.

    Args:
      object_id (str): A string used to identify an object.
    """
    buff = libplasma.get(self.conn, object_id)[0]
    return PlasmaBuffer(buff, object_id, self)

  def get_metadata(self, object_id):
    """Create a buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block until the object
    has been sealed. The retrieved buffer is immutable.

    Args:
      object_id (str): A string used to identify an object.
    """
    buff = libplasma.get(self.conn, object_id)[1]
    return PlasmaBuffer(buff, object_id, self)

  def contains(self, object_id):
    """Check if the object is present and has been sealed in the PlasmaStore.

    Args:
      object_id (str): A string used to identify an object.
    """
    return libplasma.contains(self.conn, object_id)

  def seal(self, object_id):
    """Seal the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been sealed, the buffer is immutable and can only be
    accessed through get.

    Args:
      object_id (str): A string used to identify an object.
    """
    libplasma.seal(self.conn, object_id)

  def delete(self, object_id):
    """Delete the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been deleted, the buffer is no longer accessible.

    Args:
      object_id (str): A string used to identify an object.
    """
    libplasma.delete(self.conn, object_id)

  def evict(self, num_bytes):
    """Evict some objects until to recover some bytes.

    Recover at least num_bytes bytes if possible.

    Args:
      num_bytes (int): The number of bytes to attempt to recover.
    """
    return libplasma.evict(self.conn, num_bytes)

  def transfer(self, addr, port, object_id):
    """Transfer local object with id object_id to another plasma instance

    Args:
      addr (str): IPv4 address of the plasma instance the object is sent to.
      port (int): Port number of the plasma instance the object is sent to.
      object_id (str): A string used to identify an object.
    """
    return libplasma.transfer(self.conn, object_id, addr, port)

  def fetch(self, object_ids):
    """Fetch the object with id object_id from another plasma manager instance.

    Args:
      object_id (str): A string used to identify an object.
    """
    return libplasma.fetch(self.conn, object_ids)

  def wait(self, object_ids, timeout=PLASMA_WAIT_TIMEOUT, num_returns=1):
    """Wait until num_returns objects in object_ids are ready.

    Args:
      object_ids (List[str]): List of object IDs to wait for.
      timeout (int): Return to the caller after timeout milliseconds.
      num_returns (int): We are waiting for this number of objects to be ready.

    Returns:
      ready_ids, waiting_ids (List[str], List[str]): List of object IDs that
        are ready and list of object IDs we might still wait on respectively.
    """
    ready_ids, waiting_ids = libplasma.wait(self.conn, object_ids, timeout, num_returns)
    return ready_ids, list(waiting_ids)

  def subscribe(self):
    """Subscribe to notifications about sealed objects."""
    fd = libplasma.subscribe(self.conn)
    self.notification_sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_STREAM)
    # Make the socket non-blocking.
    self.notification_sock.setblocking(0)

  def get_next_notification(self):
    """Get the next notification from the notification socket."""
    if not self.notification_sock:
      raise Exception("To get notifications, first call subscribe.")
    # Loop until we've read PLASMA_ID_SIZE bytes from the socket.
    while True:
      try:
        message_data = self.notification_sock.recv(PLASMA_ID_SIZE)
      except socket.error:
        time.sleep(0.001)
      else:
        assert len(message_data) == PLASMA_ID_SIZE
        break
    return message_data

DEFAULT_PLASMA_STORE_MEMORY = 10 ** 9

def random_name():
  return str(random.randint(0, 99999999))

def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY, use_valgrind=False, use_profiler=False):
  """Start a plasma store process.

  Args:
    use_valgrind (bool): True if the plasma store should be started inside of
      valgrind. If this is True, use_profiler must be False.
    use_profiler (bool): True if the plasma store should be started inside a
      profiler. If this is True, use_valgrind must be False.

  Return:
    A tuple of the name of the plasma store socket and the process ID of the
      plasma store process.
  """
  if use_valgrind and use_profiler:
    raise Exception("Cannot use valgrind and profiler at the same time.")
  plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/plasma_store")
  plasma_store_name = "/tmp/scheduler{}".format(random_name())
  command = [plasma_store_executable, "-s", plasma_store_name, "-m", str(plasma_store_memory)]
  if use_valgrind:
    pid = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + command)
    time.sleep(1.0)
  elif use_profiler:
    pid = subprocess.Popen(["valgrind", "--tool=callgrind"] + command)
    time.sleep(1.0)
  else:
    pid = subprocess.Popen(command)
    time.sleep(0.1)
  return plasma_store_name, pid

def start_plasma_manager(store_name, redis_address, num_retries=20, use_valgrind=False, run_profiler=False):
  """Start a plasma manager and return the ports it listens on.

  Args:
    store_name (str): The name of the plasma store socket.
    redis_address (str): The address of the Redis server.
    use_valgrind (bool): True if the Plasma manager should be started inside of
      valgrind and False otherwise.

  Returns:
    A tuple of the Plasma manager socket name, the process ID of the Plasma
      manager process, and the port that the manager is listening on.

  Raises:
    Exception: An exception is raised if the manager could not be started.
  """
  plasma_manager_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/plasma_manager")
  plasma_manager_name = "/tmp/scheduler{}".format(random_name())
  port = None
  process = None
  counter = 0
  while counter < num_retries:
    if counter > 0:
      print("Plasma manager failed to start, retrying now.")
    port = random.randint(10000, 65535)
    command = [plasma_manager_executable,
               "-s", store_name,
               "-m", plasma_manager_name,
               "-h", "127.0.0.1",
               "-p", str(port),
               "-r", redis_address]
    if use_valgrind:
      process = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + command)
    elif run_profiler:
      process = subprocess.Popen(["valgrind", "--tool=callgrind"] + command)
    else:
      process = subprocess.Popen(command)
    # This sleep is critical. If the plasma_manager fails to start because the
    # port is already in use, then we need it to fail within 0.1 seconds.
    time.sleep(0.1)
    # See if the process has terminated
    if process.poll() == None:
      return plasma_manager_name, process, port
    counter += 1
  raise Exception("Couldn't start plasma manager.")

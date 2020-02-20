import argparse
import logging

from concurrent import futures

import grpc
import redis

from ray.core.generated import dashboard_pb2
from ray.core.generated import dashboard_pb2_grpc

# TODO(sang): Move it to a different file.
NODE_INFO_CHANNEL = "NODE_INFO_CHANNEL"
RAY_INFO_CHANNEL = "RAY_INFO_CHANNEL"

logger = logging.getLogger(__file__)


class IngestServer(dashboard_pb2_grpc.DashboardServiceServicer):
    """Ingest Server that ingests User Dashboard metrics."""

    def __init__(self, redis_host, redis_port):
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

    def NodeInfoEvent(self, request, context):
        self.redis_client.publish(NODE_INFO_CHANNEL, request.json_data)
        return dashboard_pb2.NodeInfoEventReply()

    def RayletInfoEvent(self, request, context):
        self.redis_client.publish(RAY_INFO_CHANNEL, request.json_data)
        return dashboard_pb2.RayletInfoEventReply()

    def LogFileEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        return dashboard_pb2.LogFileEventReply()

    def ErrorInfoEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        return dashboard_pb2.ErrorInfoEventReply()

    def ProfilingStatusEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        return dashboard_pb2.ProfilingStatusEventReply()

    def ProfilingInfoEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        return dashboard_pb2.ProfilingInfoReply()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "dashboard to connect to."))
    parser.add_argument(
        "--host",
        required=True,
        type=str,
        help="The host to use for the GRPC server.")
    parser.add_argument(
        "--port",
        required=False,
        default=50051,
        type=str,
        help="The port to use for the GRPC server.")
    parser.add_argument(
        "--redis-address",
        required=False,
        default="127.0.0.1:6379",
        type=str,
        help="Redis address to store metrics")
    args = parser.parse_args()

    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    redis_host, redis_port = args.redis_address.strip().split(":")
    dashboard_pb2_grpc.add_DashboardServiceServicer_to_server(
        IngestServer(redis_host, redis_port), server)
    server.add_insecure_port("{}:{}".format(args.host, args.port))
    logger.info("Server listening on port {}".format(args.port))
    server.start()
    server.wait_for_termination()

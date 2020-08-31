import logging

import mimetypes

import ray.new_dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class LogAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        mimetypes.add_type("text/plain", ".err")
        mimetypes.add_type("text/plain", ".out")
        routes.static("/logs", self._dashboard_agent.log_dir, show_index=True)

    async def run(self, server):
        pass

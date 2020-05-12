import {
  createStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  Typography,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { StoreState } from "../../../store";
import Errors from "./dialogs/errors/Errors";
import Logs from "./dialogs/logs/Logs";
import NodeRowGroup from "./NodeRowGroup";
import TotalRow from "./TotalRow";
import { RayletInfoResponse, NodeInfoResponse } from '../../../../../../../../bazel-ray/python/ray/dashboard/client/src/api';

function clusterWorkerPids(rayletInfo: RayletInfoResponse): Map<string, Set<number>> {
  // Given a Raylet response, this extracts, per node, all the worker pids registered
  // with the Raylet. It returns these in a Map from node ip to set of process ids.
  const nodeMap = new Map();
  const workerPids = new Set();
  for (const [nodeIp, {workersStats}] of Object.entries(rayletInfo.nodes)) {
    for (const worker of workersStats) {
      workerPids.add(worker.pid);
    }
    nodeMap.set(nodeIp, workerPids);
  }
  return nodeMap;
}

function filterDataFromOtherClusters(rayletInfo: RayletInfoResponse, nodeInfo: NodeInfoResponse) {
  // In our current state, the NodeInfoResponse does not filter out data from workers that do not
  // belong to the cluster that this dashboard is reporting for. Thus, we use the data from the
  // Raylet to filter out workers whose process ids are not part of this cluster.
  const nodeIpToWorkerPids = clusterWorkerPids(rayletInfo);
  nodeInfo.clients = nodeInfo.clients.map(client => {
    const allowedPids = nodeIpToWorkerPids.get(client.ip)
    const filtered_workers = client.workers.filter(worker => worker.pid in allowedPids)
    client.workers = filtered_workers
    return client
  })
}

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
  });

const mapStateToProps = (state: StoreState) => ({
  nodeInfo: state.dashboard.nodeInfo,
  rayletInfo: state.dashboard.rayletInfo,
});

type State = {
  logDialog: { hostname: string; pid: number | null } | null;
  errorDialog: { hostname: string; pid: number | null } | null;
};

class NodeInfo extends React.Component<
  WithStyles<typeof styles> & ReturnType<typeof mapStateToProps>
> {
  state: State = {
    logDialog: null,
    errorDialog: null,
  };

  setLogDialog = (hostname: string, pid: number | null) => {
    this.setState({ logDialog: { hostname, pid } });
  };

  clearLogDialog = () => {
    this.setState({ logDialog: null });
  };

  setErrorDialog = (hostname: string, pid: number | null) => {
    this.setState({ errorDialog: { hostname, pid } });
  };

  clearErrorDialog = () => {
    this.setState({ errorDialog: null });
  };

  render() {
    const { classes, nodeInfo, rayletInfo } = this.props;
    const { logDialog, errorDialog } = this.state;

    if (nodeInfo === null || rayletInfo === null) {
      return <Typography color="textSecondary">Loading...</Typography>;
    }
    const workerPidsByNode: Map<string, Set<number>> = clusterWorkerPids(rayletInfo);

    const logCounts: {
      [ip: string]: {
        perWorker: {
          [pid: string]: number;
        };
        total: number;
      };
    } = {};

    const errorCounts: {
      [ip: string]: {
        perWorker: {
          [pid: string]: number;
        };
        total: number;
      };
    } = {};
    console.log('len', nodeInfo.clients.length)
    // Initialize inner structure of the count objects
    for (const client of nodeInfo.clients) {
      const allowedPids = workerPidsByNode.get(client.ip) || new Set();
      logCounts[client.ip] = { perWorker: {}, total: 0 };
      errorCounts[client.ip] = { perWorker: {}, total: 0 };
      for (const worker of client.workers) {
        if (worker.pid in allowedPids) {
          logCounts[client.ip].perWorker[worker.pid] = 0;
          errorCounts[client.ip].perWorker[worker.pid] = 0;
        }
      }
    }


    for (const ip of Object.keys(nodeInfo.log_counts)) {
      if (ip in logCounts) {
        for (const [pid, count] of Object.entries(nodeInfo.log_counts[ip])) {
          logCounts[ip].perWorker[pid] = count;
          logCounts[ip].total += count;
        }
      }
    }

    for (const ip of Object.keys(nodeInfo.error_counts)) {
      if (ip in errorCounts) {
        for (const [pid, count] of Object.entries(nodeInfo.error_counts[ip])) {
          errorCounts[ip].perWorker[pid] = count;
          errorCounts[ip].total += count;
        }
      }
    }

    return (
      <React.Fragment>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              <TableCell className={classes.cell} />
              <TableCell className={classes.cell}>Host</TableCell>
              <TableCell className={classes.cell}>Workers</TableCell>
              <TableCell className={classes.cell}>Uptime</TableCell>
              <TableCell className={classes.cell}>CPU</TableCell>
              <TableCell className={classes.cell}>RAM</TableCell>
              <TableCell className={classes.cell}>Disk</TableCell>
              <TableCell className={classes.cell}>Sent</TableCell>
              <TableCell className={classes.cell}>Received</TableCell>
              <TableCell className={classes.cell}>Logs</TableCell>
              <TableCell className={classes.cell}>Errors</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {nodeInfo.clients.map((client) => (
              <NodeRowGroup
                key={client.ip}
                node={client}
                clusterWorkerPids={workerPidsByNode.get(client.ip) || new Set()}
                raylet={
                  client.ip in rayletInfo.nodes
                    ? rayletInfo.nodes[client.ip]
                    : null
                }
                logCounts={logCounts[client.ip]}
                errorCounts={errorCounts[client.ip]}
                setLogDialog={this.setLogDialog}
                setErrorDialog={this.setErrorDialog}
                initialExpanded={nodeInfo.clients.length <= 1}
              />
            ))}
            <TotalRow
              nodes={nodeInfo.clients}
              logCounts={logCounts}
              errorCounts={errorCounts}
            />
          </TableBody>
        </Table>
        {logDialog !== null && (
          <Logs
            clearLogDialog={this.clearLogDialog}
            hostname={logDialog.hostname}
            pid={logDialog.pid}
          />
        )}
        {errorDialog !== null && (
          <Errors
            clearErrorDialog={this.clearErrorDialog}
            hostname={errorDialog.hostname}
            pid={errorDialog.pid}
          />
        )}
      </React.Fragment>
    );
  }
}

export default connect(mapStateToProps)(withStyles(styles)(NodeInfo));

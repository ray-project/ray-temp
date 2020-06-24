import {
  createStyles,
  makeStyles,
  TableCell,
  TableRow,
  Theme,
} from "@material-ui/core";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import classNames from "classnames";
import React, { useState } from "react";
import {
  NodeInfoResponse,
  NodeInfoResponseWorker,
  RayletInfoResponse,
} from "../../../api";
import { NodeWorkerRow } from "./NodeWorkerRow";

const useNodeRowGroupStyles = makeStyles((theme: Theme) =>
  createStyles({
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
    expandCollapseCell: {
      cursor: "pointer",
    },
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
    extraInfo: {
      fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
      whiteSpace: "pre",
    },
  }),
);

type ArrayType<T> = T extends Array<infer U> ? U : never;
type Node = ArrayType<NodeInfoResponse["clients"]>;

type NodeRowGroupProps = {
  node: Node;
  clusterWorkers: Array<NodeInfoResponseWorker>;
  raylet: RayletInfoResponse["nodes"][keyof RayletInfoResponse["nodes"]] | null;
  logCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  };
  errorCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  };
  setLogDialog: (hostname: string, pid: number | null) => void;
  setErrorDialog: (hostname: string, pid: number | null) => void;
  initialExpanded: boolean;
};

const NodeRowGroup: React.FC<NodeRowGroupProps> = ({
  node,
  raylet,
  clusterWorkers,
  logCounts,
  errorCounts,
  setLogDialog,
  setErrorDialog,
  initialExpanded,
}) => {
  const [expanded, setExpanded] = useState<boolean>(initialExpanded);
  const toggleExpand = () => setExpanded(!expanded);
  const classes = useNodeRowGroupStyles();
  return (
    <React.Fragment>
      <TableRow hover>
        <TableCell
          className={classNames(classes.cell, classes.expandCollapseCell)}
          onClick={toggleExpand}
        >
          {!expanded ? (
            <AddIcon className={classes.expandCollapseIcon} />
          ) : (
            <RemoveIcon className={classes.expandCollapseIcon} />
          )}
        </TableCell>
        {features.map(({ NodeFeature }, index) => (
          <TableCell className={classes.cell} key={index}>
            <NodeFeature node={node} />
          </TableCell>
        ))}
      </TableRow>
      {expanded && (
        <React.Fragment>
          {raylet !== null && raylet.extraInfo !== undefined && (
            <TableRow hover>
              <TableCell className={classes.cell} />
              <TableCell
                className={classNames(classes.cell, classes.extraInfo)}
                colSpan={features.length}
              >
                {raylet.extraInfo}
              </TableCell>
            </TableRow>
          )}
          {clusterWorkers.map((worker, index: number) => {
            const rayletWorker =
              raylet?.workersStats.find(
                (rayletWorker) => worker.pid === rayletWorker.pid,
              ) || null;
            const featureData = { rayletWorker, node, worker };
            return (
              <NodeWorkerRow
                key={index}
                features={features.map((feature) => feature.WorkerFeature)}
                data={featureData}
              />
            );
          })}
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default NodeRowGroup;

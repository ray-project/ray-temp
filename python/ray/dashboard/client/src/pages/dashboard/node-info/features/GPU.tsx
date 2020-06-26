import { Typography, Tooltip, Box, styled, Theme, makeStyles, createStyles} from "@material-ui/core";
import React from "react";
import UsageBar from "../../../../common/UsageBar";
import {RightPaddedTypography} from "../../../../common/CustomTypography";
import { getWeightedAverage, sum } from "../../../../common/util";
import { ResourceSlot, GPUStats, ResourceAllocations } from "../../../../api";
import {
  ClusterFeatureComponent,
  Node,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

const GPU_COL_WIDTH = 120;

const clusterUtilization = (nodes: Array<Node>): number => {
  const utils = nodes
    .map((node) => ({ weight: node.gpus.length, value: nodeAverageUtilization(node) }))
    .filter((util) => !isNaN(util.value));
  if (utils.length === 0) {
    return NaN;
  }
  return getWeightedAverage(utils);
};

const nodeAverageUtilization = (node: Node): number => {
  if (!node.gpus || node.gpus.length === 0) {
    return NaN;
  }
  const utilizationSum = sum(node.gpus.map((gpu) => gpu.utilization_gpu));
  const avgUtilization = utilizationSum / node.gpus.length;
  return avgUtilization;
};

export const ClusterGPU: ClusterFeatureComponent = ({ nodes }) => {
  const clusterAverageUtilization = clusterUtilization(nodes);
  return (
    <div style={{ minWidth: GPU_COL_WIDTH }}>
      {isNaN(clusterAverageUtilization) ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <UsageBar
          percent={clusterAverageUtilization}
          text={`${clusterAverageUtilization.toFixed(1)}%`}
        />
      )}
    </div>
  );
};

export const NodeGPU: NodeFeatureComponent = ({ node }) => {
  const hasGPU = (node.gpus !== undefined) && (node.gpus.length !== 0)
  return (
    <div style={{ minWidth: GPU_COL_WIDTH }}>
      {hasGPU ? (
        node.gpus.map((gpu, i) => <NodeGPUEntry gpu={gpu} slot={i} />)
      ) : (
          <Typography color="textSecondary" component="span" variant="inherit">
            N/A
          </Typography>
        )}
    </div>
  );
};

type NodeGPUEntryProps = {
  slot: number;
  gpu: GPUStats;
}

const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({ gpu, slot }) => {
  return (
    <Box display="flex" style={{minWidth: GPU_COL_WIDTH}}>
      <Tooltip title={gpu.name}>
          <RightPaddedTypography variant="h6">[{slot}]:</RightPaddedTypography>
      </Tooltip>
      <UsageBar percent={gpu.utilization_gpu} 
                text={`${gpu.utilization_gpu.toFixed(1)}%`} />
    </Box>
  )
}

type WorkerGPUEntryProps = {
  resourceSlot: ResourceSlot;
  slot: number;
}

const WorkerGPUEntry: React.FC<WorkerGPUEntryProps> = ({resourceSlot, slot}) => {
  const {allocation} = resourceSlot;
  return (
    <Typography variant="h6">[{slot}]: {allocation}</Typography>
  );
    
}

export const WorkerGPU: WorkerFeatureComponent = ({ rayletWorker }) => {
  const workerRes = rayletWorker?.coreWorkerStats.usedResources;
  const workerUsedGPUResources = workerRes?.["GPU"];
  let message;
  if (workerUsedGPUResources === undefined) {
    message = (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  } else {
    message = workerUsedGPUResources.resourceSlots.map(
        (resourceSlot, i) => <WorkerGPUEntry resourceSlot={resourceSlot} slot={i} />
      )
  }
  return <div style={{ minWidth: 60 }}>{message}</div>;
};

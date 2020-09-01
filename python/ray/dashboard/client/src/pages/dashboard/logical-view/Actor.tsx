import {
  createStyles,
  Theme,
  Typography,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import {
  ActorInfo,
  ActorState,
  checkProfilingStatus,
  CheckProfilingStatusResponse,
  getProfilingResultURL,
  isFullActorInfo,
  launchKillActor,
  launchProfiling,
} from "../../../api";
import { sum } from "../../../common/util";
import ActorDetailsPane from "./ActorDetailsPane";

const memoryDebuggingDocLink =
  "https://docs.ray.io/en/latest/memory-management.html#debugging-using-ray-memory";
const styles = (theme: Theme) =>
  createStyles({
    root: {
      borderColor: theme.palette.divider,
      borderStyle: "solid",
      borderWidth: 1,
      marginTop: theme.spacing(2),
      padding: theme.spacing(2),
      width: "100%",
    },
    title: {
      color: theme.palette.text.secondary,
      fontSize: "0.75rem",
    },
    action: {
      color: theme.palette.primary.main,
      textDecoration: "none",
      "&:hover": {
        cursor: "pointer",
      },
    },
    invalidStateTypeInfeasible: {
      color: theme.palette.error.main,
    },
    invalidStateTypePendingActor: {
      color: theme.palette.secondary.main,
    },

    webuiDisplay: {
      fontSize: "0.875rem",
    },
    inlineHTML: {
      fontSize: "0.875rem",
      display: "inline",
    },
  });

type Props = {
  actor: ActorInfo;
};

type State = {
  profiling: {
    [profilingId: string]: {
      startTime: number;
      latestResponse: CheckProfilingStatusResponse | null;
    };
  };
};

class Actor extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    profiling: {},
  };

  handleProfilingClick = (duration: number) => async () => {
    const actor = this.props.actor;
    if (actor.state === ActorState.Alive) {
      const profilingId = await launchProfiling(
        actor.nodeId,
        actor.pid,
        duration,
      );
      this.setState((state) => ({
        profiling: {
          ...state.profiling,
          [profilingId]: { startTime: Date.now(), latestResponse: null },
        },
      }));
      const checkProfilingStatusLoop = async () => {
        const response = await checkProfilingStatus(profilingId);
        this.setState((state) => ({
          profiling: {
            ...state.profiling,
            [profilingId]: {
              ...state.profiling[profilingId],
              latestResponse: response,
            },
          },
        }));
        if (response.status === "pending") {
          setTimeout(checkProfilingStatusLoop, 1000);
        }
      };
      await checkProfilingStatusLoop();
    }
  };

  killActor = () => {
    const actor = this.props.actor;
    if (actor.state === ActorState.Alive) {
      launchKillActor(actor.actorId, actor.ipAddress, actor.port);
    }
  };

  render() {
    const { classes, actor } = this.props;
    const { profiling } = this.state;
    const invalidStateType = isFullActorInfo(actor)
      ? undefined
      : actor.invalidStateType;
    const information = isFullActorInfo(actor)
      ? [
          {
            label: "Resources",
            value:
              Object.entries(actor.usedResources).length > 0 &&
              Object.entries(actor.usedResources)
                .sort((a, b) => a[0].localeCompare(b[0]))
                .map(
                  ([key, value]) =>
                    `${sum(
                      value.resourceSlots.map((slot) => slot.allocation),
                    )} ${key}`,
                )
                .join(", "),
          },
          {
            label: "Number of pending tasks",
            value: actor.taskQueueLength.toLocaleString(),
            tooltip:
              "The number of tasks that are currently pending to execute on this actor. If this number " +
              "remains consistently high, it may indicate that this actor is a bottleneck in your application.",
          },
          {
            label: "Number of executed tasks",
            value: actor.numExecutedTasks.toLocaleString(),
            tooltip:
              "The number of tasks this actor has executed throughout its lifetimes.",
          },
          {
            label: "Number of ObjectRefs in scope",
            value: actor.numObjectRefsInScope.toLocaleString(),
            tooltip:
              "The number of ObjectRefs that this actor is keeping in scope via its internal state. " +
              "This does not imply that the objects are in active use or colocated on the node with the actor " +
              `currently. This can be useful for debugging memory leaks. See the docs at ${memoryDebuggingDocLink} ` +
              "for more information.",
          },
          {
            label: "Number of local objects",
            value: actor.numLocalObjects.toLocaleString(),
            tooltip:
              "The number of small objects that this actor has stored in its local in-process memory store. This can be useful for " +
              `debugging memory leaks. See the docs at ${memoryDebuggingDocLink} for more information`,
          },
          {
            label: "Object store memory used (MiB)",
            value: actor.usedObjectStoreMemory.toLocaleString(),
            tooltip:
              "The total amount of memory that this actor is occupying in the Ray object store. " +
              "If this number is increasing without bounds, you might have a memory leak. See " +
              `the docs at: ${memoryDebuggingDocLink} for more information.`,
          },
        ]
      : [
          {
            label: "Actor ID",
            value: actor.actorId,
            tooltip: "",
          },
          {
            label: "Required resources",
            value:
              actor.requiredResources &&
              Object.entries(actor.requiredResources).length > 0 &&
              Object.entries(actor.requiredResources)
                .sort((a, b) => a[0].localeCompare(b[0]))
                .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                .join(", "),
            tooltip: "",
          },
        ];

    // Construct the custom message from the actor.
    let actorCustomDisplay: JSX.Element[] = [];
    if (isFullActorInfo(actor) && actor.webuiDisplay) {
      actorCustomDisplay = Object.keys(actor.webuiDisplay)
        .sort()
        .map((key, _, __) => {
          // Construct the value from actor.
          // Please refer to worker.py::show_in_dashboard for schema.
          const valueEncoded = actor.webuiDisplay![key];
          const valueParsed = JSON.parse(valueEncoded);
          let valueRendered = valueParsed["message"];
          if (valueParsed["dtype"] === "html") {
            valueRendered = (
              <div
                className={classes.inlineHTML}
                dangerouslySetInnerHTML={{ __html: valueRendered }}
              ></div>
            );
          }

          if (key === "") {
            return (
              <Typography className={classes.webuiDisplay}>
                &nbsp; &nbsp; {valueRendered}
              </Typography>
            );
          } else {
            return (
              <Typography className={classes.webuiDisplay}>
                &nbsp; &nbsp; {key}: {valueRendered}
              </Typography>
            );
          }
        });
    }

    return (
      <div className={classes.root}>
        <Typography className={classes.title}>
          {isFullActorInfo(actor) ? (
            <React.Fragment>
              Actor {actor.actorId} (Profile for
              {[10, 30, 60].map((duration) => (
                <React.Fragment>
                  {" "}
                  <span
                    className={classes.action}
                    onClick={this.handleProfilingClick(duration)}
                  >
                    {duration}s
                  </span>
                </React.Fragment>
              ))}
              ){" "}
              {actor.state === ActorState.Alive && (
                <span className={classes.action} onClick={this.killActor}>
                  Kill Actor
                </span>
              )}
              {Object.entries(profiling).map(
                ([profilingId, { startTime, latestResponse }]) =>
                  latestResponse !== null && (
                    <React.Fragment>
                      (
                      {latestResponse.status === "pending" ? (
                        `Profiling for ${Math.round(
                          (Date.now() - startTime) / 1000,
                        )}s...`
                      ) : latestResponse.status === "finished" ? (
                        <a
                          className={classes.action}
                          href={getProfilingResultURL(profilingId)}
                          rel="noopener noreferrer"
                          target="_blank"
                        >
                          Profiling result
                        </a>
                      ) : latestResponse.status === "error" ? (
                        `Profiling error: ${latestResponse.error.trim()}`
                      ) : undefined}
                      ){" "}
                    </React.Fragment>
                  ),
              )}
            </React.Fragment>
          ) : actor.invalidStateType === "infeasibleActor" ? (
            <span className={classes.invalidStateTypeInfeasible}>
              {actor.actorTitle} cannot be created because the Ray cluster
              cannot satisfy its resource requirements.
            </span>
          ) : (
            <span className={classes.invalidStateTypePendingActor}>
              {actor.actorTitle} is pending until resources are available.
            </span>
          )}
        </Typography>
        <ActorDetailsPane
          actorDetails={information}
          actorTitle={actor.actorTitle ?? ""}
          actorState={actor.state}
          invalidStateType={invalidStateType}
        />
        {isFullActorInfo(actor) && (
          <React.Fragment>
            {actorCustomDisplay.length > 0 && (
              <React.Fragment>{actorCustomDisplay}</React.Fragment>
            )}
          </React.Fragment>
        )}
      </div>
    );
  }
}

export default withStyles(styles)(Actor);

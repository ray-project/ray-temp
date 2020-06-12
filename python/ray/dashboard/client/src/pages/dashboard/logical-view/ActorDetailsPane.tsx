import {
  createStyles,
  Divider,
  Grid,
  makeStyles,
  Theme,
  Tooltip,
} from "@material-ui/core";
import React from "react";
import { ActorState, InvalidStateType } from "../../../api";

type ActorStateReprProps = {
  state: ActorState;
  ist?: InvalidStateType;
};

const actorStateReprStyles = makeStyles((theme: Theme) =>
  createStyles({
    infeasible: {
      color: theme.palette.error.light,
    },
    pending: {
      color: theme.palette.warning.light,
    },
    unknown: {
      color: theme.palette.warning.light,
    },
    creating: {
      color: theme.palette.success.light,
    },
    alive: {
      color: theme.palette.success.dark,
    },
    restarting: {
      color: theme.palette.warning.light,
    },
    dead: {
      color: "#cccccc",
    },
  }),
);

const ActorStateRepr = ({ state, ist }: ActorStateReprProps) => {
  const classes = actorStateReprStyles();
  const { Alive, Dead, Creating, Restarting, Invalid } = ActorState;
  switch (state) {
    case Invalid:
      if (ist === "infeasibleActor") {
        return <div className={classes.infeasible}>Infeasible</div>;
      }
      if (ist === "pendingActor") {
        return <div className={classes.pending}>Pending Resources</div>;
      }
      return <div className={classes.unknown}>Unknown</div>;
    case Creating:
      return <div className={classes.creating}>Creating</div>;
    case Alive:
      return <div className={classes.alive}>Alive</div>;
    case Restarting:
      return <div className={classes.restarting}>Restarting</div>;
    case Dead:
      return <div className={classes.dead}>Dead</div>;
  }
};

type ActorDetailsPaneProps = {
  actorTitle: string;
  invalidStateType?: InvalidStateType;
  actorState: ActorState;
  actorDetails: {
    label: string;
    value: any;
    tooltip?: string;
  }[];
};

const useStyles = makeStyles((theme: Theme) => ({
  divider: {
    width: "100%",
    margin: "0 auto",
  },
  actorTitleWrapper: {
    marginTop: ".50em",
    marginBottom: ".50em",
    fontWeight: "bold",
    fontSize: "130%",
  },
  actorTitle: {
    marginRight: "1em",
  },
  detailsPane: {
    margin: ".5em",
  },
}));

type LabeledDatumProps = {
  label: string;
  datum: any;
  tooltip?: string;
};

const labeledDatumStyles = makeStyles({
  label: {
    textDecorationLine: "underline",
    textDecorationColor: "#a6c3e3",
    textDecorationThickness: "1px",
    textDecorationStyle: "dotted",
    cursor: "help",
  },
});

const LabeledDatum = ({ label, datum, tooltip }: LabeledDatumProps) => {
  const classes = labeledDatumStyles();
  const innerHtml = (
    <Grid container item xs={6}>
      <Grid item xs={6}>
        <span className={classes.label}>{label}</span>
      </Grid>
      <Grid item xs={6}>
        <span>{datum}</span>
      </Grid>
    </Grid>
  );
  return tooltip ? <Tooltip title={tooltip}>{innerHtml}</Tooltip> : innerHtml;
};

const ActorDetailsPane = ({
  actorTitle,
  actorDetails,
  actorState,
  invalidStateType,
}: ActorDetailsPaneProps) => {
  const classes = useStyles();
  return (
    <React.Fragment>
      <div className={classes.actorTitleWrapper}>
        <div className={classes.actorTitle}>{actorTitle}</div>
        <ActorStateRepr ist={invalidStateType} state={actorState} />
      </div>
      <Divider className={classes.divider} />
      <Grid container className={classes.detailsPane}>
        {actorDetails.map(
          ({ label, value, tooltip }) =>
            value &&
            value.length > 0 && (
              <LabeledDatum label={label} datum={value} tooltip={tooltip} />
            ),
        )}
      </Grid>
    </React.Fragment>
  );
};

export default ActorDetailsPane;


import { Typography, styled } from "@material-ui/core";

export const RightPaddedTypography = styled(Typography)(({
   theme
}) => ({
  paddingRight: theme.spacing(1),
}));


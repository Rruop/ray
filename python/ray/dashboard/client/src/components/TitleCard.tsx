import { Box, Paper, SxProps, Theme } from "@mui/material";
import React, { PropsWithChildren, ReactNode } from "react";

const TitleCard = ({
  title,
  children,
  sx,
}: PropsWithChildren<{ title?: ReactNode | string; sx?: SxProps<Theme> }>) => {
  return (
    <Paper
      sx={[
        {
          padding: 2,
          paddingTop: 1.5,
          marginX: 1,
          marginY: 2,
        },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
      elevation={0}
    >
      {title && (
        <Box
          sx={(theme) => ({
            fontSize: theme.typography.fontSize + 2,
            fontWeight: 500,
            color: theme.palette.text.secondary,
            marginBottom: 1,
          })}
        >
          {title}
        </Box>
      )}
      <Box sx={{ flex: 1, display: "flex", flexDirection: "column" }}>{children}</Box>
    </Paper>
  );
};

export default TitleCard;

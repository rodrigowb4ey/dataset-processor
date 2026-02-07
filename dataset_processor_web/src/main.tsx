import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "@fontsource/source-serif-4/600.css";
import "@fontsource/source-serif-4/700.css";
import "@fontsource/space-grotesk/400.css";
import "@fontsource/space-grotesk/500.css";
import "@fontsource/space-grotesk/700.css";
import "@mantine/core/styles.css";
import "@mantine/notifications/styles.css";
import { MantineProvider, createTheme } from "@mantine/core";
import { Notifications } from "@mantine/notifications";

import App from "./App";
import "./index.css";

const theme = createTheme({
  primaryColor: "teal",
  fontFamily: "'Space Grotesk', sans-serif",
  headings: {
    fontFamily: "'Source Serif 4', serif",
  },
  defaultRadius: "md",
  colors: {
    teal: [
      "#edfef9",
      "#d8f7ef",
      "#adefdc",
      "#7be6c8",
      "#54dfb6",
      "#3bdba8",
      "#2ed9a0",
      "#1fc08a",
      "#11ab79",
      "#009467",
    ],
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MantineProvider theme={theme} defaultColorScheme="light">
      <Notifications position="top-right" />
      <App />
    </MantineProvider>
  </StrictMode>,
);

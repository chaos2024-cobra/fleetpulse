import express from "express";

const app = express();
const port = Number(process.env.PORT ?? 3001);

app.disable("x-powered-by");
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "fleetpulse" });
});

app.get("/", (_req, res) => {
  res.json({
    name: "FleetPulse",
    status: "scaffold",
    health: "/health",
  });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`FleetPulse server listening on port ${port}`);
});

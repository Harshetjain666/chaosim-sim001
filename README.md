# SIM-001: ChaoSim Core Simulation Engine

A lightweight prototype of ChaoSim's core runtime engine and Adaptive Scenario Director.

It ingests behavioral telemetry, maintains an authoritative in-memory world, computes operator stress on a fixed tick, mutates the environment when conditions deteriorate, and seals the final state with an HMAC-SHA256 fingerprint.

The written architecture answers for Challenge 1 and Challenge 2, plus the design rationale and the path to production, live in [docs/ARCHITECTURE_BRIEF.md](docs/ARCHITECTURE_BRIEF.md).

## Requirements

Node 23.6 or newer. That is the only dependency. The engine runs straight from TypeScript source (Node strips the types) and the cryptography uses the built in `node:crypto` module, so there is nothing to install and nothing to compile.

On Node 22.6 through 23.5, add the `--experimental-strip-types` flag, or run any command through `npx tsx` instead of `node`.

## Quick start

```bash
node src/index.ts        # run the deterministic chaos load test and print the report
node src/index.ts --trace  # same, with a per second metrics table
node src/index.ts --live   # drive the engine in real time for ~30 seconds
node --test                # run the unit suite
```

The default command runs the three phase Chaos Load Test, prints a phase by phase summary, emits the CEO alert when the trap trips, outputs the verified fingerprint, and finishes with a block of acceptance checks. It exits non zero if any check fails, so it doubles as a regression gate.

## What the run shows

* **Phase 1 (0s to 10s).** Stable typing, no backspacing. The stress curve stays dormant, panic sits at zero, the director never acts.
* **Phase 2 (10s to 20s).** Friction ramps in. Velocity climbs first, then backspacing spikes. Panic begins rising but stays under the trap threshold of 75.
* **Phase 3 (20s to 30s).** The failure cascade sustains the pressure, panic crosses 75, the Scenario Director fires exactly once: codebase health drops by 30, CEO anxiety jumps to 95, and the alert is emitted. On completion the final state is frozen, timestamped, and signed.

## Project layout

```
src/
  types.ts             WorldState and telemetry types
  config.ts            engine tunables in one place
  worldState.ts        authoritative state factory (spec initial values)
  telemetryWindow.ts   per session sliding window over raw events
  stressModel.ts       metric summarization and the panic stress curve
  scenarioDirector.ts  the trap: fires once, mutates the world, emits the alert
  engine.ts            the authoritative fixed tick engine (pure step function)
  attestation.ts       canonical serialize, HMAC-SHA256 sign, constant time verify
  simulation.ts        lifecycle façade: step then complete()
  liveLoop.ts          fixed timestep real time driver (no clock drift)
  scenario.ts          synthetic telemetry generator for the load test
  chaosLoadTest.ts     the three phase scenario plus acceptance checks
  index.ts             CLI entry and report formatting
test/
  sim.test.ts          determinism, trap once, stress curve, attestation
docs/
  ARCHITECTURE_BRIEF.md  Challenge 1 and 2 answers, design notes, production path
```

## Design in one paragraph

The whole engine reduces to one idea: the world only ever changes inside `SimulationEngine.step()`, and that method is a pure function of the previous state and the telemetry batch for the tick. No wall clock leaks into the math, no client is trusted to compute anything, and the only side channel out is the signed attestation. Because the tick is pure and deterministic, the live loop, the chaos test, and the unit tests all drive the identical logic, and a given seed always reproduces the same trajectory. Everything else (the sliding window, the stress curve, the director, the signer) hangs off that spine.

## Configuration

`CHAOSIM_SECRET` sets the HMAC key (defaults to a development key). Pass `--seed=N` to vary the synthetic telemetry while keeping the run reproducible. Pass `--fixed-time` to stamp a constant completion time so the fingerprint itself is reproducible across runs.

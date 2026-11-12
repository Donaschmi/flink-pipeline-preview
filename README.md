# Flink Pipeline Preview

An IntelliJ IDEA plugin that draws the Apache Flink DataStream pipeline built by the Java file you are editing, live, in a tool window. The graph comes from static analysis of the code, so nothing has to compile or run.

It supports the DataStream API of Flink 1.x and 2.x, plus the DataStream V2 API of Flink 2.x. It is tested against Flink 1.20.5 and 2.3.0 and requires IntelliJ IDEA 2025.3 or later.

## Try it

1. Open this folder in IntelliJ IDEA as a Gradle project. JDK 21 is required.
2. Run `./gradlew runIde`. The first run downloads IntelliJ IDEA 2025.3 (about 1.5 GB).
3. In the IDE that opens, open `examples/flink-job` (a small Gradle project that depends on Flink 1.20.5).
4. Open `FraudDetectionJob.java`, then the **Flink Pipeline** tool window on the right.

To install the plugin in your everyday IDE instead, run `./gradlew buildPlugin`. Then use **Settings → Plugins → ⚙ → Install Plugin from Disk…** with `build/distributions/flink-pipeline-preview-0.1.0.zip`.

To build against an IDE you already have installed rather than downloading one, pass its path: `./gradlew runIde -PplatformLocalPath=/path/to/idea`.

## Using the view

- The graph updates about 350 ms after you stop typing, and when you switch files.
- Clicking a node opens the code that created it. The node under the caret is outlined.
- Labels on edges show partitioning (`keyBy(...)`, `rebalance`, `broadcast`). Dashed edges are side outputs; dotted edges are broadcasts.
- Small tags on a node show what was folded into it: `window(TumblingEventTimeWindows(1min))`, `connect`, the parallelism (`×4`), and chaining changes.
- `.name("...")` becomes the node title. Hover a node to see uid, parallelism, slot sharing group and source location.
- Drag to pan and scroll to zoom. **Fit** re-centres the graph, **→ / ↓** switches between top-down and left-to-right layout, and **?** shows the legend.

## What the analysis follows

- Fluent chains, local variables and reassignments (`s = s.map(...)`), fields, and `cond ? a : b`.
- Helper methods in your project, up to 4 levels deep, with stream parameters bound to the caller's arguments. This works across files too.
- `union`, `connect`, `join`, `coGroup` and `intervalJoin`; windows; side outputs (`getSideOutput`); broadcast state; static helpers such as `AsyncDataStream.unorderedWait`.
- Configuration calls (`name`, `uid`, `setParallelism`, `slotSharingGroup`, `disableChaining`, `startNewChain`, and the V2 `with*` methods) annotate the node they apply to instead of adding one.
- Calls that don't fully type-check while you type still resolve to their best candidate.

**Which methods are shown.** If the file has a `main`, or other pipeline methods that nothing else in the file calls, those are drawn as the job. If the caret is in one of those methods, only that one is drawn. If it is in a helper, the method that calls the helper is drawn.

## Limitations

- **Java only, with Flink on the classpath.** Flink classes have to resolve, because the analyzer recognises them by package (`org.apache.flink.streaming.api.datastream`, `org.apache.flink.datastream.api`).
- **Straight-line view of control flow.** `if` branches are not split into alternatives, and loops are read as a single pass.
- **Unresolvable streams become "external input" nodes.** This covers streams that are method parameters with no caller in the file, or values built by reflection.
- **Not the runtime graph.** It shows the logical DataStream graph, not Flink's job graph. Chaining is not computed, and the Table API and SQL are not shown.

## Project layout

- `src/main/kotlin/.../analysis/` holds the static analysis. `FlinkPipelineAnalyzer` is the core, `FlinkApi` contains the Flink-specific knowledge (method and package names), and `PipelineScope` decides which methods to show.
- `src/main/kotlin/.../ui/` holds the tool window. It runs the analysis in the background, renders through JCEF, and handles navigation.
- `src/main/resources/flinkpreview/view.html` renders the graph and includes its own layered layout, so it needs no network access or libraries.
- `src/test/` contains the analyzer tests. Some use small stand-in Flink classes, and `RealFlinkApiTest` runs the example job against the real Flink 1.20.5 and 2.3.0 jars. Run them with `./gradlew test`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Read AGENTS.md first

[`AGENTS.md`](AGENTS.md) is the authoritative source for build commands, pre-commit
checks, testing, PR title conventions, the AI-tooling disclosure requirement, and the
most common pitfalls. **Do not duplicate or contradict it.** The essentials it covers:

- **Build order matters**: the Java/Scala build links against native shared libraries
  produced by the C++ build. Always build the native backend *before* running Maven, or
  you get a build that compiles but fails at runtime (JNI/Substrait mismatch).
- Always invoke Maven through `./build/mvn` (pins Maven version + JVM flags), never bare `mvn`.
- Before committing run `./dev/format-scala-code.sh`, `./dev/format-cpp-code.sh`
  (clang-format **15.x only**), and `./dev/check.py header main --fix`.

This file adds the cross-cutting architecture that requires reading many files to grasp.

## The big picture

Gluten is a **Spark plugin** that offloads Spark SQL execution to a native engine
(Velox or ClickHouse) without changing user SQL/DataFrame code. The flow:

```
Spark physical plan
  → (Gluten extensions rewrite the plan, replacing supported operators with Transformers)
  → Substrait plan  (cross-language compute spec, the JVM↔native contract)
  → JNI call into the native backend
  → native operator chain executes
  → results returned as Arrow-backed ColumnarBatch
  → Spark's Columnar API (Spark 3.0+) consumes them
```

Two boundaries dominate everything:
1. **The Substrait plan** is the serialization contract between JVM and native. Operators
   exist on *both* sides — a Scala `*Transformer` that emits Substrait, and a native operator
   that consumes it. Changing one without the other breaks at runtime, not compile time.
2. **The JNI boundary** (`cpp/core/jni`, `cpp/velox/jni`) and **off-heap memory** are shared
   between the JVM and native. Gluten uses unified memory management and the Apache Arrow
   format as the on-the-wire columnar representation.

**Fallback**: unsupported operators run in vanilla Spark. Gluten inserts ColumnarToRow (C2R)
and RowToColumnar (R2C) transitions to bridge between its columnar format and Spark's
internal row format at fallback boundaries.

## Module map

JVM side (Maven modules, see root `pom.xml`):

| Module | Role |
|---|---|
| `gluten-substrait` | Substrait plan construction; the `*Transformer` operator classes that emit Substrait. The JVM half of the JVM↔native contract. |
| `gluten-core` | Core plugin glue, planner rules/extensions, columnar APIs. Editing this **requires a native rebuild** (see AGENTS.md). |
| `gluten-arrow` | Arrow-format data interchange between JVM and native. |
| `backends-velox` / `backends-clickhouse` | Backend-specific JVM glue, operator support, and config. Pick exactly one via `-Pbackends-velox` / `-Pbackends-clickhouse`. |
| `shims/spark33..spark41` + `shims/common` | Per-Spark-version compatibility. Gluten targets the latest 3–4 Spark releases (3.3, 3.4, 3.5, 4.0, 4.1); version-specific code lives here, not in core. |
| `gluten-delta`, `gluten-iceberg`, `gluten-hudi`, `gluten-paimon`, `gluten-kafka` | Table-format / source integrations (profile-gated). |
| `gluten-celeborn`, `gluten-uniffle`, `gluten-disaggregated-shuffle` | Remote/columnar shuffle backends. |
| `gluten-ui` | Spark UI extensions and metrics display. |
| `gluten-ut` | Unit tests, including the re-run Spark UTs. **Add tests here.** |
| `package` | Assembles the final bundled Gluten JAR (output under `package/target/`). |

Native side (`cpp/`, CMake):

| Path | Role |
|---|---|
| `cpp/core` | Backend-agnostic native core: `jni` (JNI entry points), `compute`, `memory`, `operators`, `shuffle`, `config`. |
| `cpp/velox` | Velox backend: `substrait` (Substrait→Velox plan), `compute`, `operators`, `shuffle`, `udf`, `benchmarks`, plus cloud-FS (`filesystem`) and `cudf` GPU bits. |
| `cpp-ch/` | ClickHouse backend native code. |
| `ep/` | External project builds (Velox, ClickHouse) pulled and built by the `dev/` scripts. |

## Key commands

Full matrix and flags live in `AGENTS.md` and [docs/get-started/Velox.md](docs/get-started/Velox.md).
Quick reference:

```bash
# Velox: one-shot native + bundled JAR
export PROMPT_ALWAYS_RESPOND=y
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON \
  --enable_s3=OFF --enable_gcs=OFF --enable_hdfs=OFF --enable_abfs=OFF

# Velox: native only (incremental); add --build_tests=ON --build_benchmarks=ON --build_type=Debug to debug
./dev/builddeps-veloxbe.sh

# Rebuild only the Gluten C++ layer after editing gluten-core / gluten-substrait
./dev/builddeps-veloxbe.sh build_gluten_cpp

# Run gluten-ut tests — backend + Spark + Scala profiles are all required
./build/mvn test -pl gluten-ut -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pscala-2.12

# After a clean checkout, install modules to the local repo once so siblings resolve
./build/mvn install -Pbackends-velox -Pspark-3.5 -Pscala-2.12 -DskipTests
```

**Spark 4.x requires JDK 17+**: pair `-Pspark-4.0`/`-Pspark-4.1` with
`-Pjava-17 -Pscala-2.13 -Dmaven.compiler.release=17`.

## Where to add support for a new function/operator

Because operators are split across the Substrait boundary, adding support usually touches:
the JVM `*Transformer` (in `gluten-substrait` or the backend module) that emits Substrait,
the native operator/expression (in `cpp/velox` or `cpp-ch`) that consumes it, and a test in
`gluten-ut`. The support matrices in `docs/velox-backend-*-function-support.md` track what
exists. See [docs/developers/velox-function-development-guide.md](docs/developers/velox-function-development-guide.md).

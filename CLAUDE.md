# dataflow

Typed pipeline lib (Java 21, Gradle, Simplified Annotations). Java-8-Streams shape.

## Stage hierarchy

`Stage<I,O>` sealed (`stage/Stage.java`), 4 non-sealed permits:
- `SourceStage<O>` - `()->O` (input = `DataTypes.NONE`)
- `FilterStage<T>` - `List<T>->List<T>` (subset)
- `TransformStage<I,O>` - 1:1 (also `T->BOOLEAN` predicates, `I->JSON_OBJECT` via `ObjectBuildTransform`)
- `CollectStage<I,O>` - terminal reduction (incl. `MapCollect`: named fan-out `I->Map<String,Object>`)

## Concrete stage template

- `@StageSpec(id, displayName, description, category)` on the class. `id` is the wire `"kind"`.
- Exactly one canonical `public static @NotNull XStage of(...)` whose parameters all carry `@Configurable` (or a zero-arg `of()`). Schema, `config()` and the load factory are derived from it by `StageReflection` - no `fromConfig`, no hand-written `FieldSpec`. Convenience overloads must have an unannotated parameter, or the lookup is ambiguous.
- Implements `inputType/outputType/summary/execute`
- `@Getter(style = NamingStyle.FLUENT) @RequiredArgsConstructor(access=PRIVATE)` (or `@NoArgsConstructor(PRIVATE)` stateless)
- `if (input == null) return null;` (rejection semantics)
- List outputs: `Concurrent.newUnmodifiableList(...)`. Maps whose order is declared (named outputs, tables, results): `Concurrent.newUnmodifiableLinkedMap(...)`, never `Map.copyOf`.
- Regex stages: `Pattern.compile(...)` cached in `of(...)`
- Validate everything in `of` and throw `IllegalArgumentException`: bodies, operands, enum-like strings, ranges. `of` runs on load, so a refusal fails `PipelineGson.fromJson` (wrapped in the reflection library's exception - assert on the root cause).
- JSON text is written with `PipelineGson.gson()` (no HTML escaping).

## Configuration slots

The factory parameter's type picks the `FieldSpec.Type`:

| Parameter | Type | Wire |
|---|---|---|
| `String` / `int` / `long` / `double` / `boolean` (boxed too) | `STRING` / `INT` / `LONG` / `DOUBLE` / `BOOLEAN` | JSON primitive; `INT` / `LONG` read exactly (a fraction or out-of-range value fails the load) |
| `DataType<?>` | `DATA_TYPE` | label |
| `List<? extends Stage<?, ?>>` / `Chain` | `SUB_PIPELINE` | stage array, no source |
| `Map<String, List<...>>` / `NamedChains` | `SUB_PIPELINES_MAP` | object of stage arrays |
| `Map<String, TypedChain<?>>` | `TYPED_SUB_PIPELINES_MAP` | object of `{outputType, chain}` |
| `DataPipeline<?>` | `PIPELINE` | stage array whose stage 0 is a source (a pipeline file) |
| `Map<String, String>` | `STRING_MAP` | object of strings, document order |

- **The field named after a parameter holds that slot's config value** - `config()` reads it back. A stage that parses a string into another type stores the parsed value under a different name, or renames the parameter and keeps the wire key with `@Configurable(name = "...")`. `StageFieldConventionTest` enforces this for every registered stage.
- Optional slot: `@Configurable(optional = true)` on a `@Nullable` parameter; absent on the wire means `null`.
- `placeholder` must be a value `of` accepts: `PipelineSerdeTest.everyNonChainKindFactoryRoundTrips` builds each stage's default config from it (a `STRING_MAP` placeholder is a JSON object literal; `PIPELINE` stages are skipped).
- Adding a `FieldSpec.Type` breaks every exhaustive `switch` over it downstream (the Discord UI's `StageFields`).

## Pipeline operands

A `PIPELINE` slot carries a whole sourced pipeline that reads a second document (`JoinByKeyTransform.right`, `KeyLookupTransform.table`, `ConcatTransform.other`).
- In `of`: `DataPipeline.validate(expectedOutputType)`; on failure throw `IllegalArgumentException("Invalid <Class> operand: " + report.issues())`. Store it under the parameter's name (a narrowed generic is fine).
- In `execute`: `ctx.evaluateOperand(operand)` - runs it against the same context at most once per context, keyed by instance identity, `null` held too, reentrant for nested operands, a self-reaching operand throws `Pipeline operand cycle detected`, a throwing evaluation holds nothing, a read from a second thread waits for the evaluation under way, and every new `PipelineContext` starts with an empty memo - so a host builds one context per run.
- The value is shared by every read in the run: **never mutate it** - copy rows before changing them.
- A value derived from an operand once per run (an index, say) is itself a small `DataPipeline` over the operand, evaluated through the same memo (`RowKeys.index`).

## Naming (suffix = role)

`XxxSource | XxxFilter | XxxTransform | XxxPredicate | XxxCollect`. Filter/Predicate paired by name (`ContainsFilter` <-> `ContainsPredicate`).

Packages under `stage/`: `source`, `filter/{string,list,numeric,dom,json}`, `transform/{string,primitive,list,dom,json,encoding}`, `predicate/{string,numeric,dom,json,common}`, `terminal/{collect,sum,average,minmax,match}`.

## Registry / category

`StageRegistry` scans `dev.simplified.dataflow.stage` (test classes included) for `Stage` subtypes carrying `@StageSpec` and indexes them by `id`; a duplicate id fails the static initialiser. **Renaming an id breaks stored JSON.** Adding a stage: the class with `@StageSpec` in the package its `StageSpec.Category` names - nothing else registers it.

`StageSpec.Category` declaration order: `SOURCE` first, `TERMINAL_*` last, rest alphabetical. UI relies on `ordinal()`.

## Sub-pipelines

A sourceless body chain is a `chain.Chain`. Variants:
- Single body (`Map`, `FlatMap`, `SortBy`, `Min/MaxBy`, `DistinctBy`, `*Match`, `FindFirst`, `Take/DropWhile`, `ReplaceMatch`, `Coalesce`, binary arithmetic, `Zip`, `Rotate`, `Broadcast`): `FieldSpec.Type.SUB_PIPELINE` + `StageConfig.subPipeline/getSubPipeline` returning `Chain`.
- Named bodies (`MapCollect`, `And/OrPredicate`): `FieldSpec.Type.SUB_PIPELINES_MAP` + `subPipelines/getSubPipelines` returning `chain.NamedChains`.
- Typed named bodies (`ObjectBuildTransform`): `FieldSpec.Type.TYPED_SUB_PIPELINES_MAP` + `typedSubPipelines/getTypedSubPipelines` returning `Map<String, chain.TypedChain>`.

`Chain` owns: `validate(seed, body, expected)`, `execute(ctx, input)`, `of(stages)`, `builder()`. Per-stage walks call `T result = this.body.execute(ctx, element)` directly. Validate every body in `of`; on failure throw `IllegalArgumentException("Invalid <Class> body: " + report.issues())`. An empty body fails `Chain.validate`. A named branch that yields `null` omits its name (`MapCollect`, `ObjectBuildTransform`).

## DataPipeline

- `Builder.build()` validates eagerly, throws `IllegalStateException` on bad chain
- `Builder.validate()` returns `ValidationReport`, no throw
- `DataPipeline.validate(DataType)` also reports a last stage that does not produce the given type (operand check)
- `execute(ctx)` does NOT re-validate (build-time guarantee)
- `DataPipeline.empty().execute(ctx)` returns `null`

## DataType

Sealed: `Basic<T>`, `ListType<E>`, `SetType<E>`. **Identity by `label()`** - `RAW_HTML` ≠ `STRING` despite both being `String`-backed. Parameterised via `DataTypes.byLabel("List<INT>")`.

Sort/Min/Max key types restricted to `INT, LONG, FLOAT, DOUBLE, STRING`; others rejected at build time.

## Fetching

Stages fetch through `ctx.fetcher()` (client `UrlFetcher`), never a client of their own. An optional `maxBodyBytes` (`@Nullable Long`) calls the capped overload only when set, so the fetcher's configured cap still applies otherwise. Every error status raises: catch `UrlFetchException.ClientError` (400-451) by type to tell a 4xx apart - `RateLimited` carries a synthetic `429` and is not one. An error status raises its own type whatever the size of its body; only a success body past the cap raises `BodyCapExceeded`.

## Serde / test

- Wire format: `{"kind":"X", ...config}` via `serde/PipelineGson`. Round-tripped by `PipelineSerdeTest`.
- Tests: JUnit 5 + Hamcrest, one assertion per behavior, in the test package matching the stage's. Every stage gets a wire round trip (build -> `toJson` -> `fromJson` -> equal config and output). `PipelineContext.defaults()` for default fetcher / NOOP resolver.
- `StageCatalogTest` pins id, class and category; add a row for a new stage.
- Fixture stages for framework tests live in `src/test/.../stage/fixture` and register on the test classpath.
- `gradle test` runs; `gradle compileJava compileTestJava` builds.

## Commits

Imperative title <70 chars, per-feature where possible.

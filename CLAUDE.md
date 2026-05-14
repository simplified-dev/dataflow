# dataflow - project guide

Typed pipeline library modeled after Java 8 Streams. Java 21, Gradle, Lombok.

## Stage model

`Stage<I, O>` is sealed (`stage/Stage.java`), permits 5 non-sealed sub-interfaces:
- `SourceStage<O>` - `() -> O` (input type defaults to `DataTypes.NONE`)
- `FilterStage<T>` - `List<T> -> List<T>` (subset)
- `TransformStage<I, O>` - `I -> O` (1:1 mapping; also used for `T -> BOOLEAN` predicates)
- `CollectStage<I, O>` - terminal reduction, normally `List<T> -> X`
- `BranchStage<I>` - terminal fan-out `I -> Map<String, Object>`

Each concrete stage:
- `public static @NotNull XStage of(...)` factory
- `public static @NotNull XStage fromConfig(StageConfig cfg)` for serde
- Implements `inputType()`, `outputType()`, `kind()`, `summary()`, `config()`, `execute(ctx, input)`
- Lombok: `@Getter @Accessors(fluent = true) @RequiredArgsConstructor(access = PRIVATE)`
  (or `@NoArgsConstructor(PRIVATE)` for stateless)
- Null-input guard: `if (input == null) return null;` (rejection semantics)
- List results wrapped via `Concurrent.newUnmodifiableList(...)`

## Naming

Suffix convention (commit `60ff9d9`):
- `XxxSource` - source
- `XxxFilter` - `List<T> -> List<T>`
- `XxxTransform` - 1:1 map
- `XxxPredicate` - `T -> BOOLEAN`
- `XxxCollect` - terminal

`Filter` and `Predicate` are paired by name (`StringContainsFilter` / `StringContainsPredicate`).

## Packages (`stage/`)

```
source/                 UrlSource, PasteSource, OfSource, OfListSource, EmbedSource
filter/{string,list,numeric,dom,json}/
transform/{string,primitive,list,dom,json,encoding}/
predicate/{string,numeric,dom,json,common}/
terminal/               Branch
terminal/{collect,sum,average,minmax,match}/
```

Type-grouped subpackages (string/dom/json/numeric) for breadth; semantic groups (collect /
sum / minmax / match) for terminals.

## StageKind + StageCategory

`StageKind` (enum, `stage/StageKind.java`) is the wire-format discriminator. Each constant
carries display name, type signature, `StageCategory`, `List<FieldSpec>` schema, and
`Function<StageConfig, Stage<?, ?>>` factory. **Renaming a constant invalidates stored
pipeline JSON** - safe pre-production, but require an explicit migration once anything is
persisted.

When adding a stage: new `StageKind` constant + matching imports + correct `StageCategory`.

`StageCategory`: UI palette grouping. After the round-1/2 reorg: `SOURCE`, `BRANCH`,
`FILTER_*`, `TRANSFORM_*`, `PREDICATE_*`, `TERMINAL_{COLLECT,SUM,AVERAGE,MINMAX,MATCH}`.

## Sub-pipeline bodies

Stages that run a sub-chain per element (`Map`, `FlatMap`, `SortBy`, `MinBy`, `MaxBy`,
`AnyMatch`, `AllMatch`, `NoneMatch`, `FindFirst`, `TakeWhile`, `DropWhile`) carry the body
via `FieldType.SUB_PIPELINE` + `StageConfig.subPipeline(name, chain)` /
`getSubPipeline(name)`.

Branch and `AndPredicate`/`OrPredicate` (multiple named chains) use
`FieldType.SUB_PIPELINES_MAP` + `subPipelines()` / `getSubPipelines()`.

Validate body type chains at build time:
`StageChainValidator.validate(seedInputType, body, expectedOutputType)` returns a
`ValidationReport`. Throw `IllegalArgumentException("Invalid <stage> body: " + report.issues())`
on failure.

## DataPipeline lifecycle

- `Builder.build()` validates eagerly and throws `IllegalStateException` on invalid chain
- `Builder.validate()` returns a `ValidationReport` without throwing (for inspection)
- `execute(ctx)` no longer re-validates; build-time guarantees it's well-typed
- Empty pipeline (`DataPipeline.empty()`) returns `null` on execute, doesn't throw

## DataType

`DataType<T>` is sealed (`stage/../DataType.java`): `Basic<T>`, `ListType<E>`, `SetType<E>`.
Identity is by `label()` (so `RAW_HTML` and `STRING` are distinct despite both being
`String`-backed). `DataTypes.byLabel("List<INT>")` constructs parameterised types.

Supported `Comparable` keys for `Sort*` / `Min*` / `Max*`: `INT`, `LONG`, `FLOAT`, `DOUBLE`,
`STRING`. Stages reject other key types at build time.

## Build / test

- Gradle Java 21 toolchain (see `build.gradle.kts`)
- Deps: `simplified-dev:client` (UrlFetcher), `simplified-dev:collections` (Concurrent*),
  `simplified-dev:gson-extras`, jsoup, gson, jackson-xml, slf4j
- Test: JUnit 5 + Hamcrest. One assertion-per-behavior style.
- `gradle compileJava compileTestJava` builds main + tests; `gradle test` runs.

## Conventions

- Javadoc: imports, no FQN refs. Single hyphens never em dashes. `{@link X}` not `{@code X}`
  for cross-references. `{@inheritDoc}` on every interface override.
- `@NotNull` / `@Nullable` from `org.jetbrains.annotations` on params and returns.
- Regex stages cache their `Pattern` at construction (`Pattern.compile(regex)` in `of(...)`).
- Wire format unchanged: stages serialise to `{"kind": "X", ...config fields}` arrays via
  `PipelineGson` (`serde/PipelineGson.java`). Round-tripped by `PipelineSerdeTest`.
- `PipelineContext.empty()` creates a default-fetcher / NOOP-resolver context for tests.
- Commits: imperative title under ~70 chars. Per-feature where possible. No
  `Co-Authored-By:` lines.

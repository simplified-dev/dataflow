# dataflow

Typed pipeline lib (Java 21, Gradle, Lombok). Java-8-Streams shape.

## Stage hierarchy

`Stage<I,O>` sealed (`stage/Stage.java`), 4 non-sealed permits:
- `SourceStage<O>` - `()->O` (input = `DataTypes.NONE`)
- `FilterStage<T>` - `List<T>->List<T>` (subset)
- `TransformStage<I,O>` - 1:1 (also `T->BOOLEAN` predicates, `I->JSON_OBJECT` via `JsonObjectBuildTransform`)
- `CollectStage<I,O>` - terminal reduction (incl. `MapCollect`: named fan-out `I->Map<String,Object>`)

## Concrete stage template

- `public static @NotNull XStage of(...)` factory
- `public static @NotNull XStage fromConfig(StageConfig)` for serde
- Implements `inputType/outputType/kind/summary/config/execute`
- `@Getter @Accessors(fluent=true) @RequiredArgsConstructor(access=PRIVATE)` (or `@NoArgsConstructor(PRIVATE)` stateless)
- `if (input == null) return null;` (rejection semantics)
- List outputs: `Concurrent.newUnmodifiableList(...)`
- Regex stages: `Pattern.compile(...)` cached in `of(...)`

## Naming (suffix = role)

`XxxSource | XxxFilter | XxxTransform | XxxPredicate | XxxCollect`. Filter/Predicate paired by name (`StringContainsFilter` ↔ `StringContainsPredicate`).

Packages under `stage/`: `source`, `filter/{string,list,numeric,dom,json}`, `transform/{string,primitive,list,dom,json,encoding}`, `predicate/{string,numeric,dom,json,common}`, `terminal` (+ `collect,sum,average,minmax,match`).

## StageKind / StageCategory

`StageKind` (enum) = wire-format discriminator carrying schema + factory. **Renaming a constant breaks stored JSON** - safe pre-prod; require migration once persisted. Adding a stage: new `StageKind` + correct `StageCategory`.

`StageCategory` declaration order: `SOURCE` first, `TERMINAL_*` last, rest alphabetical. UI relies on `ordinal()`.

## Sub-pipelines

A sourceless body chain is a `chain.Chain`. Variants:
- Single body (`Map`, `FlatMap`, `SortBy`, `Min/MaxBy`, `*Match`, `FindFirst`, `Take/DropWhile`): `FieldType.SUB_PIPELINE` + `StageConfig.subPipeline/getSubPipeline` returning `Chain`.
- Named bodies (`MapCollect`, `And/OrPredicate`): `FieldType.SUB_PIPELINES_MAP` + `subPipelines/getSubPipelines` returning `chain.NamedChains`.
- Typed named bodies (`JsonObjectBuildTransform`): `FieldType.TYPED_SUB_PIPELINES_MAP` + `typedSubPipelines/getTypedSubPipelines` returning `Map<String, chain.TypedChain>`.

`Chain` owns: `validate(seed, body, expected)`, `execute(ctx, input)`, `of(stages)`, `builder()`. Per-stage walks call `T result = this.body.execute(ctx, element)` directly. Validate at build time; on failure throw `IllegalArgumentException("Invalid <stage> body: " + report.issues())`.

## DataPipeline

- `Builder.build()` validates eagerly, throws `IllegalStateException` on bad chain
- `Builder.validate()` returns `ValidationReport`, no throw
- `execute(ctx)` does NOT re-validate (build-time guarantee)
- `DataPipeline.empty().execute(ctx)` returns `null`

## DataType

Sealed: `Basic<T>`, `ListType<E>`, `SetType<E>`. **Identity by `label()`** - `RAW_HTML` ≠ `STRING` despite both being `String`-backed. Parameterised via `DataTypes.byLabel("List<INT>")`.

Sort/Min/Max key types restricted to `INT, LONG, FLOAT, DOUBLE, STRING`; others rejected at build time.

## Serde / test

- Wire format: `{"kind":"X", ...config}` via `serde/PipelineGson`. Round-tripped by `PipelineSerdeTest`.
- Tests: JUnit 5 + Hamcrest, one assertion per behavior. `PipelineContext.defaults()` for default fetcher / NOOP resolver.
- `gradle test` runs; `gradle compileJava compileTestJava` builds.

## Commits

Imperative title <70 chars, per-feature where possible.

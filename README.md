# dataflow

Standalone, Discord-free Java library for authoring **typed data-extraction pipelines**
over arbitrary HTML / XML / JSON / text input.

Pipelines are flat sequences of stages - `Source -> (Filter | Transform | Predicate)* -> Terminal` -
with optional `Branch` (named multi-output) and `EmbedSource` (run a saved pipeline as a
single source-like stage). Modeled after Java 8 Streams; the stage taxonomy mirrors Stream
operation categories.

## Install

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.github.simplified-dev:dataflow:master-SNAPSHOT")
}
```

Requires Java 21.

## Quick start

```java
import dev.sbs.dataflow.*;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.transform.dom.*;       // ParseHtml, CssSelect, NodeText, NthChild
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;

DataPipeline pipeline = DataPipeline.builder()
    .source(UrlSource.rawHtml("https://hypixelskyblock.minecraft.wiki/w/Dark_Claymore"))
    .stage(ParseHtmlTransform.of())
    .stage(CssSelectTransform.of("table.infobox tr"))
    .stage(DomTextContainsFilter.of("Dmg"))
    .stage(FirstCollect.of(DataTypes.DOM_NODE))
    .stage(NthChildTransform.of("td", 1))
    .stage(NodeTextTransform.of())
    .stage(RegexExtractTransform.of("\\d+"))
    .stage(ParseIntTransform.of())
    .build();

Integer dmg = pipeline.execute(PipelineContext.defaults()); // 500 - generic <T> infers Integer
```

### Package layout

```
dev.sbs.dataflow.stage
  .source                 UrlSource, LiteralSource, LiteralListSource, EmbedSource
  .filter.string          Contains, Matches, StartsWith, EndsWith, Equals, NonEmpty
  .filter.list            Distinct, NotNull, Take, Skip, IndexInRange, TakeWhile, DropWhile
  .filter.numeric         Int/Long/Double x {GreaterThan, LessThan, InRange}
  .filter.dom             DomTextContains, DomTextMatches, DomHasAttr, DomTagEquals
  .filter.json            JsonHasField, JsonFieldEquals
  .transform.string       LowerCase, UpperCase, StringLength, Prefix, Suffix,
                          Trim, Replace, Split, RegexExtract
  .transform.primitive    ParseInt/Long/Float/Double/Boolean, Abs*, Negate*, ToString, Peek
  .transform.list         ListLength, Reverse, Map, FlatMap, Flatten, Sort, SortBy
  .transform.dom          ParseHtml, CssSelect, NodeText, NodeAttr, NthChild,
                          DomChildren, DomParent, DomOuterHtml, DomOwnText
  .transform.json         ParseXml, ParseJson, JsonPath, JsonField,
                          JsonAsString/Int/Long/Double/Boolean, JsonStringify
  .transform.encoding     Base64Encode/Decode, UrlEncode/Decode
  .predicate.string       Contains, Matches, StartsWith, EndsWith, Equals, NonEmpty
  .predicate.numeric      Int/Long/Double x {GreaterThan, LessThan, InRange}
  .predicate.dom          DomTextContains, DomTextMatches, DomHasAttr, DomTagEquals
  .predicate.json         JsonHasField, JsonFieldEquals
  .predicate.common       NotNull, Not, And, Or
  .terminal               Branch
  .terminal.collect       First, Last, List, Set, Join
  .terminal.sum           Count, SumInt, SumLong, SumDouble
  .terminal.average       AverageInt, AverageLong, AverageDouble
  .terminal.minmax        Min, Max, MinBy, MaxBy
  .terminal.match         AnyMatch, AllMatch, NoneMatch, FindFirst
```

## Stage catalog

DataTypes: `NONE`, `RAW_HTML`, `RAW_XML`, `RAW_JSON`, `STRING`, `INT`, `LONG`,
`FLOAT`, `DOUBLE`, `BOOLEAN`, `DOM_NODE`, `JSON_ELEMENT`, `JSON_OBJECT`, `JSON_ARRAY`,
`BRANCH_OUTPUT`, plus `List<X>` / `Set<X>` constructors.

### Sources

| Kind                       | Input | Output                  | Notes                              |
|----------------------------|-------|-------------------------|------------------------------------|
| `SOURCE_URL`               | NONE  | `RAW_*`                 | shared `UrlFetcher`, body cap 5 MiB |
| `SOURCE_LITERAL`                | NONE  | `T`                     | literal value parsed from config   |
| `SOURCE_LITERAL_LIST`           | NONE  | `List<T>`               | literal list from JSON array       |
| `SOURCE_EMBED`             | NONE  | declared at construction| resolves saved pipeline by id      |

### Parse / DOM / JSON / encoding transforms

| Kind                       | Input            | Output             |
|----------------------------|------------------|--------------------|
| `PARSE_HTML`               | `RAW_HTML`       | `DOM_NODE`         |
| `PARSE_XML`                | `RAW_XML`        | `JSON_ELEMENT`     |
| `PARSE_JSON`               | `RAW_JSON`       | `JSON_ELEMENT`     |
| `TRANSFORM_CSS_SELECT`     | `DOM_NODE`       | `List<DOM_NODE>`   |
| `TRANSFORM_NODE_TEXT`      | `DOM_NODE`       | `STRING`           |
| `TRANSFORM_NODE_ATTR`      | `DOM_NODE`       | `STRING`           |
| `TRANSFORM_NTH_CHILD`      | `DOM_NODE`       | `DOM_NODE`         |
| `TRANSFORM_DOM_CHILDREN`   | `DOM_NODE`       | `List<DOM_NODE>`   |
| `TRANSFORM_DOM_PARENT`     | `DOM_NODE`       | `DOM_NODE`         |
| `TRANSFORM_DOM_OUTER_HTML` | `DOM_NODE`       | `STRING`           |
| `TRANSFORM_DOM_OWN_TEXT`   | `DOM_NODE`       | `STRING`           |
| `TRANSFORM_JSON_PATH`      | `JSON_ELEMENT`   | `JSON_ELEMENT`     |
| `TRANSFORM_JSON_FIELD`     | `JSON_OBJECT`    | `JSON_ELEMENT`     |
| `TRANSFORM_JSON_AS_STRING` | `JSON_ELEMENT`   | `STRING`           |
| `TRANSFORM_JSON_AS_INT`    | `JSON_ELEMENT`   | `INT`              |
| `TRANSFORM_JSON_AS_LONG`   | `JSON_ELEMENT`   | `LONG`             |
| `TRANSFORM_JSON_AS_DOUBLE` | `JSON_ELEMENT`   | `DOUBLE`           |
| `TRANSFORM_JSON_AS_BOOLEAN`| `JSON_ELEMENT`   | `BOOLEAN`          |
| `TRANSFORM_JSON_STRINGIFY` | `JSON_ELEMENT`   | `STRING`           |
| `TRANSFORM_BASE64_ENCODE`  | `STRING`         | `STRING`           |
| `TRANSFORM_BASE64_DECODE`  | `STRING`         | `STRING`           |
| `TRANSFORM_URL_ENCODE`     | `STRING`         | `STRING`           |
| `TRANSFORM_URL_DECODE`     | `STRING`         | `STRING`           |

### String / list / numeric / utility transforms

| Kind                       | Input          | Output            | Notes                            |
|----------------------------|----------------|-------------------|----------------------------------|
| `TRANSFORM_REGEX_EXTRACT`  | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_TRIM`           | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_REPLACE`        | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_SPLIT`          | `STRING`       | `List<STRING>`    |                                  |
| `TRANSFORM_LOWERCASE`      | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_UPPERCASE`      | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_STRING_LENGTH`  | `STRING`       | `INT`             |                                  |
| `TRANSFORM_PREFIX`         | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_SUFFIX`         | `STRING`       | `STRING`          |                                  |
| `TRANSFORM_PARSE_INT`      | `STRING`       | `INT`             |                                  |
| `TRANSFORM_PARSE_LONG`     | `STRING`       | `LONG`            |                                  |
| `TRANSFORM_PARSE_FLOAT`    | `STRING`       | `FLOAT`           |                                  |
| `TRANSFORM_PARSE_DOUBLE`   | `STRING`       | `DOUBLE`          |                                  |
| `TRANSFORM_PARSE_BOOLEAN`  | `STRING`       | `BOOLEAN`         |                                  |
| `TRANSFORM_LIST_LENGTH`    | `List<T>`      | `INT`             |                                  |
| `TRANSFORM_REVERSE`        | `List<T>`      | `List<T>`         |                                  |
| `TRANSFORM_SORT`           | `List<T>`      | `List<T>`         | natural ordering, asc/desc       |
| `TRANSFORM_SORT_BY`        | `List<T>`      | `List<T>`         | key-extractor body (`T -> K`)    |
| `TRANSFORM_MAP`            | `List<X>`      | `List<Y>`         | body `X -> Y` per element        |
| `TRANSFORM_FLAT_MAP`       | `List<X>`      | `List<Y>`         | body `X -> List<Y>`, concatenated|
| `TRANSFORM_FLATTEN`        | `List<List<T>>`| `List<T>`         |                                  |
| `TRANSFORM_ABS_INT`        | `INT`          | `INT`             |                                  |
| `TRANSFORM_ABS_LONG`       | `LONG`         | `LONG`            |                                  |
| `TRANSFORM_ABS_FLOAT`      | `FLOAT`        | `FLOAT`           |                                  |
| `TRANSFORM_ABS_DOUBLE`     | `DOUBLE`       | `DOUBLE`          |                                  |
| `TRANSFORM_NEGATE_INT`     | `INT`          | `INT`             |                                  |
| `TRANSFORM_NEGATE_LONG`    | `LONG`         | `LONG`            |                                  |
| `TRANSFORM_NEGATE_FLOAT`   | `FLOAT`        | `FLOAT`           |                                  |
| `TRANSFORM_NEGATE_DOUBLE`  | `DOUBLE`       | `DOUBLE`          |                                  |
| `TRANSFORM_TO_STRING`      | `T`            | `STRING`          |                                  |
| `TRANSFORM_PEEK`           | `T`            | `T`               | identity + `ctx.log()` side effect|

### Filters

| Kind                          | Input                   | Output                  | Notes                          |
|-------------------------------|-------------------------|-------------------------|--------------------------------|
| `FILTER_DOM_TEXT_CONTAINS`    | `List<DOM_NODE>`        | `List<DOM_NODE>`        |                                |
| `FILTER_DOM_TEXT_MATCHES`     | `List<DOM_NODE>`        | `List<DOM_NODE>`        |                                |
| `FILTER_DOM_HAS_ATTR`         | `List<DOM_NODE>`        | `List<DOM_NODE>`        |                                |
| `FILTER_DOM_TAG_EQUALS`       | `List<DOM_NODE>`        | `List<DOM_NODE>`        |                                |
| `FILTER_STRING_CONTAINS`      | `List<STRING>`          | `List<STRING>`          |                                |
| `FILTER_STRING_MATCHES`       | `List<STRING>`          | `List<STRING>`          |                                |
| `FILTER_STRING_STARTS_WITH`   | `List<STRING>`          | `List<STRING>`          |                                |
| `FILTER_STRING_ENDS_WITH`     | `List<STRING>`          | `List<STRING>`          |                                |
| `FILTER_STRING_EQUALS`        | `List<STRING>`          | `List<STRING>`          |                                |
| `FILTER_STRING_NON_EMPTY`     | `List<STRING>`          | `List<STRING>`          |                                |
| `FILTER_JSON_HAS_FIELD`       | `List<JSON_OBJECT>`     | `List<JSON_OBJECT>`     |                                |
| `FILTER_JSON_FIELD_EQUALS`    | `List<JSON_OBJECT>`     | `List<JSON_OBJECT>`     |                                |
| `FILTER_INT_GREATER_THAN`     | `List<INT>`             | `List<INT>`             |                                |
| `FILTER_INT_LESS_THAN`        | `List<INT>`             | `List<INT>`             |                                |
| `FILTER_INT_IN_RANGE`         | `List<INT>`             | `List<INT>`             |                                |
| `FILTER_LONG_GREATER_THAN`    | `List<LONG>`            | `List<LONG>`            |                                |
| `FILTER_LONG_LESS_THAN`       | `List<LONG>`            | `List<LONG>`            |                                |
| `FILTER_LONG_IN_RANGE`        | `List<LONG>`            | `List<LONG>`            |                                |
| `FILTER_DOUBLE_GREATER_THAN`  | `List<DOUBLE>`          | `List<DOUBLE>`          |                                |
| `FILTER_DOUBLE_LESS_THAN`     | `List<DOUBLE>`          | `List<DOUBLE>`          |                                |
| `FILTER_DOUBLE_IN_RANGE`      | `List<DOUBLE>`          | `List<DOUBLE>`          |                                |
| `FILTER_NOT_NULL`             | `List<T>`               | `List<T>`               |                                |
| `FILTER_TAKE`                 | `List<T>`               | `List<T>`               | first n                        |
| `FILTER_SKIP`                 | `List<T>`               | `List<T>`               | drop first n                   |
| `FILTER_INDEX_IN_RANGE`       | `List<T>`               | `List<T>`               | `[from, to)`                   |
| `FILTER_DISTINCT`             | `List<T>`               | `List<T>`               |                                |
| `FILTER_TAKE_WHILE`           | `List<T>`               | `List<T>`               | predicate body (`T -> BOOLEAN`)|
| `FILTER_DROP_WHILE`           | `List<T>`               | `List<T>`               | predicate body (`T -> BOOLEAN`)|

### Predicates (`T -> BOOLEAN`)

Single-element analogues of the filter family. Use them as the body of match collectors,
`TakeWhile` / `DropWhile`, `FindFirst`, `SortBy`, etc.

| Kind                              | Input          | Output     |
|-----------------------------------|----------------|------------|
| `PREDICATE_STRING_CONTAINS`       | `STRING`       | `BOOLEAN`  |
| `PREDICATE_STRING_STARTS_WITH`    | `STRING`       | `BOOLEAN`  |
| `PREDICATE_STRING_ENDS_WITH`      | `STRING`       | `BOOLEAN`  |
| `PREDICATE_STRING_EQUALS`         | `STRING`       | `BOOLEAN`  |
| `PREDICATE_STRING_MATCHES`        | `STRING`       | `BOOLEAN`  |
| `PREDICATE_STRING_NON_EMPTY`      | `STRING`       | `BOOLEAN`  |
| `PREDICATE_INT_GREATER_THAN`      | `INT`          | `BOOLEAN`  |
| `PREDICATE_INT_LESS_THAN`         | `INT`          | `BOOLEAN`  |
| `PREDICATE_INT_IN_RANGE`          | `INT`          | `BOOLEAN`  |
| `PREDICATE_LONG_GREATER_THAN`     | `LONG`         | `BOOLEAN`  |
| `PREDICATE_LONG_LESS_THAN`        | `LONG`         | `BOOLEAN`  |
| `PREDICATE_LONG_IN_RANGE`         | `LONG`         | `BOOLEAN`  |
| `PREDICATE_DOUBLE_GREATER_THAN`   | `DOUBLE`       | `BOOLEAN`  |
| `PREDICATE_DOUBLE_LESS_THAN`      | `DOUBLE`       | `BOOLEAN`  |
| `PREDICATE_DOUBLE_IN_RANGE`       | `DOUBLE`       | `BOOLEAN`  |
| `PREDICATE_DOM_TEXT_CONTAINS`     | `DOM_NODE`     | `BOOLEAN`  |
| `PREDICATE_DOM_TEXT_MATCHES`      | `DOM_NODE`     | `BOOLEAN`  |
| `PREDICATE_DOM_HAS_ATTR`          | `DOM_NODE`     | `BOOLEAN`  |
| `PREDICATE_DOM_TAG_EQUALS`        | `DOM_NODE`     | `BOOLEAN`  |
| `PREDICATE_JSON_HAS_FIELD`        | `JSON_OBJECT`  | `BOOLEAN`  |
| `PREDICATE_JSON_FIELD_EQUALS`     | `JSON_OBJECT`  | `BOOLEAN`  |
| `PREDICATE_NOT_NULL`              | `T`            | `BOOLEAN`  |
| `PREDICATE_NOT`                   | `BOOLEAN`      | `BOOLEAN`  |
| `PREDICATE_AND`                   | `T`            | `BOOLEAN`  |
| `PREDICATE_OR`                    | `T`            | `BOOLEAN`  |

`AND` / `OR` carry a `SUB_PIPELINES_MAP` of named predicate bodies and short-circuit.

### Terminals

| Kind                       | Input            | Output                 | Notes                              |
|----------------------------|------------------|------------------------|------------------------------------|
| `COLLECT_FIRST`            | `List<T>`        | `T`                    |                                    |
| `COLLECT_LAST`             | `List<T>`        | `T`                    |                                    |
| `COLLECT_LIST`             | `List<T>`        | `List<T>`              | identity terminal marker           |
| `COLLECT_SET`              | `List<T>`        | `Set<T>`               |                                    |
| `COLLECT_JOIN`             | `List<STRING>`   | `STRING`               |                                    |
| `COLLECT_COUNT`            | `List<T>`        | `INT`                  |                                    |
| `COLLECT_SUM_INT`          | `List<INT>`      | `INT`                  |                                    |
| `COLLECT_SUM_LONG`         | `List<LONG>`     | `LONG`                 |                                    |
| `COLLECT_SUM_DOUBLE`       | `List<DOUBLE>`   | `DOUBLE`               |                                    |
| `COLLECT_AVERAGE_INT`      | `List<INT>`      | `DOUBLE`               |                                    |
| `COLLECT_AVERAGE_LONG`     | `List<LONG>`     | `DOUBLE`               |                                    |
| `COLLECT_AVERAGE_DOUBLE`   | `List<DOUBLE>`   | `DOUBLE`               |                                    |
| `COLLECT_MIN`              | `List<T>`        | `T`                    | natural ordering                   |
| `COLLECT_MAX`              | `List<T>`        | `T`                    | natural ordering                   |
| `COLLECT_MIN_BY`           | `List<T>`        | `T`                    | key-extractor body (`T -> K`)      |
| `COLLECT_MAX_BY`           | `List<T>`        | `T`                    | key-extractor body (`T -> K`)      |
| `COLLECT_FIND_FIRST`       | `List<T>`        | `T`                    | predicate body (`T -> BOOLEAN`)    |
| `COLLECT_ANY_MATCH`        | `List<T>`        | `BOOLEAN`              | predicate body                     |
| `COLLECT_ALL_MATCH`        | `List<T>`        | `BOOLEAN`              | predicate body                     |
| `COLLECT_NONE_MATCH`       | `List<T>`        | `BOOLEAN`              | predicate body                     |
| `BRANCH`                   | `I`              | `Map<String, Object>`  | named sub-chains                   |

## Persisting a pipeline

```java
String json = PipelineGson.toJson(pipeline);   // round-trip-safe wire format
DataPipeline rebuilt = PipelineGson.fromJson(json);
```

The wire format is a top-level JSON array of stage descriptors, each carrying a `"kind"`
discriminator plus its configuration fields.

## Embedding a saved pipeline as a stage

```java
DataPipeline outer = DataPipeline.builder()
    .source(EmbedSource.of("wiki_dmg", DataTypes.INT))   // resolves at execute-time
    .build();

PipelineContext ctx = PipelineContext.builder()
    .withResolver(myDatabaseResolver)   // host-supplied DataPipelineResolver
    .build();

outer.execute(ctx);
```

`PipelineContext` enforces cycle detection: A -> A or A -> B -> A throw with a
breadcrumb of every active id.

## Validation

`DataPipeline.Builder.build()` validates the type chain eagerly and throws on any issue.
Use `Builder.validate()` to inspect a report on a still-under-construction builder:

```java
ValidationReport report = DataPipeline.builder()
    .source(LiteralSource.text("hi"))
    .stage(ParseHtmlTransform.of())  // expects RAW_HTML, got STRING
    .validate();
if (!report.isValid()) {
    for (ValidationReport.Issue issue : report.issues())
        System.err.println("stage #" + issue.stageIndex() + ": " + issue.message());
}
```

Diagnostic example: `Stage #1 (PARSE_HTML) expects input RAW_HTML but previous
stage produced STRING`.

Sub-pipeline bodies (used by `Map`, `FlatMap`, match collectors, `TakeWhile` / `DropWhile`,
`SortBy` / `MinBy` / `MaxBy`, `And` / `Or`) are wrapped as a `chain.Chain` and validated the
same way via `Chain.validate(seed, body, expected)` at build time, against the declared
element-type-in and output-type-out.

## Status

v0.1, pre-release. The Stream-parity catalog (FlatMap family, terminals, match collectors,
predicates, comparators, literal sources) is complete. Async / reactive `Stage` execution
remains deferred.

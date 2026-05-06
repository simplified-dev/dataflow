# dataflow

Standalone, Discord-free Java library for authoring **typed data-extraction pipelines**
over arbitrary HTML / XML / JSON / text input.

Pipelines are flat sequences of stages - `Source -> (Filter | Transform)* -> Collect` -
with optional `Branch` (named multi-output) and `PipelineEmbed` (run a saved pipeline as a
single stage).

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
import dev.sbs.dataflow.stage.collect.FirstCollect;
import dev.sbs.dataflow.stage.filter.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.*;

DataPipeline pipeline = DataPipeline.builder()
    .source(UrlSource.html("https://hypixelskyblock.minecraft.wiki/w/Dark_Claymore"))
    .stage(ParseHtmlTransform.create())
    .stage(CssSelectTransform.of("table.infobox tr"))
    .stage(DomTextContainsFilter.of("Dmg"))
    .stage(FirstCollect.of(DataTypes.DOM_NODE))
    .stage(NthChildTransform.of("td", 1))
    .stage(NodeTextTransform.create())
    .stage(RegexExtractTransform.of("\\d+"))
    .stage(ParseIntTransform.create())
    .build();

Integer dmg = pipeline.execute(PipelineContext.empty()); // 500 — generic <T> infers Integer
```

## Stage catalog

DataTypes: `NONE`, `RAW_HTML`, `RAW_XML`, `RAW_JSON`, `RAW_TEXT`, `STRING`, `INT`, `LONG`,
`FLOAT`, `DOUBLE`, `BOOLEAN`, `DOM_NODE`, `JSON_ELEMENT`, `JSON_OBJECT`, `JSON_ARRAY`,
`BRANCH_OUTPUT`, plus `List<X>` / `Set<X>` constructors.

### Sources

| Kind                       | Input | Output                 | Notes                              |
|----------------------------|-------|------------------------|------------------------------------|
| `SOURCE_URL`               | NONE  | `RAW_*`                | jdk `HttpClient`, body cap 5 MiB   |
| `SOURCE_PASTE`             | NONE  | `RAW_*`                | inline body string                 |
| `PIPELINE_EMBED`           | NONE  | declared at construction| resolves saved pipeline by id     |

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

### String / list / numeric transforms

| Kind                       | Input          | Output            |
|----------------------------|----------------|-------------------|
| `TRANSFORM_REGEX_EXTRACT`  | `STRING`       | `STRING`          |
| `TRANSFORM_TRIM`           | `STRING`       | `STRING`          |
| `TRANSFORM_REPLACE`        | `STRING`       | `STRING`          |
| `TRANSFORM_SPLIT`          | `STRING`       | `List<STRING>`    |
| `TRANSFORM_LOWERCASE`      | `STRING`       | `STRING`          |
| `TRANSFORM_UPPERCASE`      | `STRING`       | `STRING`          |
| `TRANSFORM_STRING_LENGTH`  | `STRING`       | `INT`             |
| `TRANSFORM_PREFIX`         | `STRING`       | `STRING`          |
| `TRANSFORM_SUFFIX`         | `STRING`       | `STRING`          |
| `TRANSFORM_PARSE_INT`      | `STRING`       | `INT`             |
| `TRANSFORM_PARSE_LONG`     | `STRING`       | `LONG`            |
| `TRANSFORM_PARSE_FLOAT`    | `STRING`       | `FLOAT`           |
| `TRANSFORM_PARSE_DOUBLE`   | `STRING`       | `DOUBLE`          |
| `TRANSFORM_PARSE_BOOLEAN`  | `STRING`       | `BOOLEAN`         |
| `TRANSFORM_LIST_LENGTH`    | `List<T>`      | `INT`             |
| `TRANSFORM_REVERSE`        | `List<T>`      | `List<T>`         |
| `TRANSFORM_ABS_INT`        | `INT`          | `INT`             |
| `TRANSFORM_ABS_LONG`       | `LONG`         | `LONG`            |
| `TRANSFORM_ABS_FLOAT`      | `FLOAT`        | `FLOAT`           |
| `TRANSFORM_ABS_DOUBLE`     | `DOUBLE`       | `DOUBLE`          |
| `TRANSFORM_NEGATE_INT`     | `INT`          | `INT`             |
| `TRANSFORM_NEGATE_LONG`    | `LONG`         | `LONG`            |
| `TRANSFORM_NEGATE_FLOAT`   | `FLOAT`        | `FLOAT`           |
| `TRANSFORM_NEGATE_DOUBLE`  | `DOUBLE`       | `DOUBLE`          |
| `TRANSFORM_TO_STRING`      | `T`            | `STRING`          |

### Filters

| Kind                          | Input                   | Output                  |
|-------------------------------|-------------------------|-------------------------|
| `FILTER_DOM_TEXT_CONTAINS`    | `List<DOM_NODE>`        | `List<DOM_NODE>`        |
| `FILTER_DOM_TEXT_MATCHES`     | `List<DOM_NODE>`        | `List<DOM_NODE>`        |
| `FILTER_DOM_HAS_ATTR`         | `List<DOM_NODE>`        | `List<DOM_NODE>`        |
| `FILTER_DOM_TAG_EQUALS`       | `List<DOM_NODE>`        | `List<DOM_NODE>`        |
| `FILTER_STRING_CONTAINS`      | `List<STRING>`          | `List<STRING>`          |
| `FILTER_STRING_MATCHES`       | `List<STRING>`          | `List<STRING>`          |
| `FILTER_STRING_STARTS_WITH`   | `List<STRING>`          | `List<STRING>`          |
| `FILTER_STRING_ENDS_WITH`     | `List<STRING>`          | `List<STRING>`          |
| `FILTER_STRING_EQUALS`        | `List<STRING>`          | `List<STRING>`          |
| `FILTER_STRING_NON_EMPTY`     | `List<STRING>`          | `List<STRING>`          |
| `FILTER_JSON_HAS_FIELD`       | `List<JSON_OBJECT>`     | `List<JSON_OBJECT>`     |
| `FILTER_JSON_FIELD_EQUALS`    | `List<JSON_OBJECT>`     | `List<JSON_OBJECT>`     |
| `FILTER_INT_GREATER_THAN`     | `List<INT>`             | `List<INT>`             |
| `FILTER_INT_LESS_THAN`        | `List<INT>`             | `List<INT>`             |
| `FILTER_INT_IN_RANGE`         | `List<INT>`             | `List<INT>`             |
| `FILTER_DOUBLE_GREATER_THAN`  | `List<DOUBLE>`          | `List<DOUBLE>`          |
| `FILTER_DOUBLE_LESS_THAN`     | `List<DOUBLE>`          | `List<DOUBLE>`          |
| `FILTER_DOUBLE_IN_RANGE`      | `List<DOUBLE>`          | `List<DOUBLE>`          |
| `FILTER_NOT_NULL`             | `List<T>`               | `List<T>`               |
| `FILTER_TAKE`                 | `List<T>`               | `List<T>`               |
| `FILTER_SKIP`                 | `List<T>`               | `List<T>`               |
| `FILTER_INDEX_IN_RANGE`       | `List<T>`               | `List<T>`               |
| `FILTER_DISTINCT`             | `List<T>`               | `List<T>`               |

### Collect

| Kind                       | Input            | Output                 |
|----------------------------|------------------|------------------------|
| `COLLECT_FIRST`            | `List<T>`        | `T`                    |
| `COLLECT_LAST`             | `List<T>`        | `T`                    |
| `COLLECT_LIST`             | `List<T>`        | `List<T>`              |
| `COLLECT_SET`              | `List<T>`        | `Set<T>`               |
| `COLLECT_JOIN`             | `List<STRING>`   | `STRING`               |

### Compound

| Kind             | Input | Output                | Notes                            |
|------------------|-------|-----------------------|----------------------------------|
| `BRANCH`         | `I`   | `Map<String, Object>` | named sub-chains                 |
| `PIPELINE_EMBED` | NONE  | declared              | resolves saved pipeline by id    |

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
    .source(PipelineEmbed.of("wiki_dmg", DataTypes.INT))   // resolves at execute-time
    .build();

PipelineContext ctx = PipelineContext.builder()
    .withResolver(myDatabaseResolver)   // host-supplied DataPipelineResolver
    .build();

outer.execute(ctx);
```

`PipelineContext` enforces cycle detection: A -> A or A -> B -> A throw with a
breadcrumb of every active id.

## Validation

```java
ValidationReport report = pipeline.validate();
if (!report.isValid()) {
    for (ValidationReport.Issue issue : report.issues())
        System.err.println("stage #" + issue.stageIndex() + ": " + issue.message());
}
```

Diagnostic example: `Stage #3 (TRANSFORM_PARSE_INT) expects input STRING but previous
stage produced List<DOM_NODE>`.

## Status

v0.1, pre-release. v2 work tracked separately:
- `TRANSFORM_MAP` (apply an inner sub-pipeline per element).
- Branch sub-chains containing branches (depth > 1).
- Embedded pipelines that consume an outer-stage value as their first input.

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
import dev.sbs.dataflow.stage.collect.CollectFirst;
import dev.sbs.dataflow.stage.filter.FilterDomTextContains;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.*;

DataPipeline pipeline = DataPipeline.builder()
    .source(UrlSource.html("https://hypixelskyblock.minecraft.wiki/w/Dark_Claymore"))
    .stage(ParseHtmlTransform.create())
    .stage(TransformCssSelect.of("table.infobox tr"))
    .stage(FilterDomTextContains.of("Dmg"))
    .stage(CollectFirst.of(DataTypes.DOM_NODE))
    .stage(TransformNthChild.of("td", 1))
    .stage(TransformNodeText.create())
    .stage(TransformRegexExtract.of("\\d+"))
    .stage(TransformParseInt.create())
    .build();

Integer dmg = (Integer) pipeline.execute(PipelineContext.empty()); // 500
```

## Stage catalog

| Kind                       | Input type        | Output type             | Notes                                |
|----------------------------|-------------------|-------------------------|--------------------------------------|
| `SOURCE_URL`               | `NONE`            | `RAW_*`                 | jdk `HttpClient`, body cap 5 MiB     |
| `SOURCE_PASTE`             | `NONE`            | `RAW_*`                 | inline body string                   |
| `PARSE_HTML`               | `RAW_HTML`        | `DOM_NODE`              | jsoup                                |
| `PARSE_XML`                | `RAW_XML`         | `JSON_ELEMENT`          | XmlMapper -> Gson bridge             |
| `PARSE_JSON`               | `RAW_JSON`        | `JSON_ELEMENT`          | Gson `JsonParser`                    |
| `TRANSFORM_CSS_SELECT`     | `DOM_NODE`        | `List<DOM_NODE>`        | jsoup CSS selector                   |
| `TRANSFORM_NODE_TEXT`      | `DOM_NODE`        | `STRING`                | `Element.text()`                     |
| `TRANSFORM_NODE_ATTR`      | `DOM_NODE`        | `STRING`                | `Element.attr(name)`                 |
| `TRANSFORM_NTH_CHILD`      | `DOM_NODE`        | `DOM_NODE`              | child selector + 0-based index       |
| `TRANSFORM_JSON_PATH`      | `JSON_ELEMENT`    | `JSON_ELEMENT`          | dot-notation walk                    |
| `TRANSFORM_JSON_FIELD`     | `JSON_OBJECT`     | `JSON_ELEMENT`          | named field                          |
| `TRANSFORM_REGEX_EXTRACT`  | `STRING`          | `STRING`                | first match or capture group         |
| `TRANSFORM_PARSE_INT`      | `STRING`          | `INT`                   | null on bad input                    |
| `TRANSFORM_PARSE_DOUBLE`   | `STRING`          | `DOUBLE`                | null on bad input                    |
| `TRANSFORM_TRIM`           | `STRING`          | `STRING`                |                                      |
| `TRANSFORM_REPLACE`        | `STRING`          | `STRING`                | regex replaceAll                     |
| `TRANSFORM_SPLIT`          | `STRING`          | `List<STRING>`          | regex delimiter                      |
| `FILTER_DOM_TEXT_CONTAINS` | `List<DOM_NODE>`  | `List<DOM_NODE>`        | keep if `text().contains(needle)`    |
| `FILTER_DISTINCT`          | `List<T>`         | `List<T>`               | LinkedHashSet semantics              |
| `COLLECT_FIRST`            | `List<T>`         | `T`                     | null on empty                        |
| `COLLECT_LAST`             | `List<T>`         | `T`                     | null on empty                        |
| `COLLECT_LIST`             | `List<T>`         | `List<T>`               | identity terminator                  |
| `COLLECT_SET`              | `List<T>`         | `Set<T>`                | first-occurrence order               |
| `COLLECT_JOIN`             | `List<STRING>`    | `STRING`                | configurable separator               |
| `BRANCH`                   | `I`               | `Map<String, Object>`   | named sub-chains                     |
| `PIPELINE_EMBED`           | `NONE`            | declared at construction| resolves saved pipeline by id        |

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

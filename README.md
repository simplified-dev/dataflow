# dataflow

Standalone, Discord-free Java library for authoring **typed data-extraction pipelines**
over arbitrary HTML / XML / JSON / Lua / text input.

Pipelines are flat sequences of stages - `Source -> (Filter | Transform | Predicate)* -> Terminal` -
with sub-pipeline bodies (per-element `Map`, named fan-out through `COLLECT_MAP` and
`TRANSFORM_JSON_OBJECT_BUILD`), pipeline operands that read a second document beside the running
value, and `EmbedSource` (run a saved pipeline as a single source-like stage). Modeled after
Java 8 Streams; the stage taxonomy mirrors Stream operation categories.

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
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.filter.dom.TextContainsFilter;
import dev.simplified.dataflow.stage.source.UrlSource;
import dev.simplified.dataflow.stage.terminal.collect.FirstCollect;
import dev.simplified.dataflow.stage.transform.dom.CssSelectTransform;
import dev.simplified.dataflow.stage.transform.dom.NthChildTransform;
import dev.simplified.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.simplified.dataflow.stage.transform.dom.TextTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;

DataPipeline<Integer> pipeline = DataPipeline.builder()
    .source(UrlSource.rawHtml("https://hypixelskyblock.minecraft.wiki/w/Dark_Claymore"))
    .stage(ParseHtmlTransform.of())
    .stage(CssSelectTransform.of("table.infobox tr"))
    .stage(TextContainsFilter.of("Dmg"))
    .stage(FirstCollect.of(DataTypes.DOM_NODE))
    .stage(NthChildTransform.of("td", 1))
    .stage(TextTransform.of())
    .stage(RegexExtractTransform.of("\\d+"))
    .stage(ParseIntTransform.of())
    .build();

Integer dmg = pipeline.execute(PipelineContext.defaults()); // 500
```

### Package layout

Class names drop their role suffix here (`Contains` is `ContainsFilter` under `.filter` and
`ContainsPredicate` under `.predicate`).

```
dev.simplified.dataflow.stage
  .source                 Url, Literal, LiteralList, Embed
  .filter.string          Contains, Matches, StartsWith, EndsWith, Equals, NonEmpty
  .filter.list            Distinct, DistinctBy, NotNull, Take, Skip, IndexInRange,
                          TakeWhile, DropWhile, Where
  .filter.numeric         Int/Long/Double x {GreaterThan, LessThan, InRange}
  .filter.dom             TextContains, TextMatches, HasAttr, TagEquals
  .filter.json            HasField, FieldEquals
  .transform.string       LowerCase, UpperCase, CamelCase, PascalCase, SnakeCase, TitleCase,
                          Length, Prepend, Append, Trim, Replace, ReplaceMatch, Split,
                          RegexExtract, ValueMap, RangeExpand, Fetch
  .transform.primitive    ParseInt/Long/Float/Double/Boolean/Roman, Abs*, Negate*,
                          Arithmetic*, BinaryArithmetic*, RoundFloat/Double, Constant,
                          Coalesce, ToString, ToRaw, Peek, Expect (+ ArithmeticOperator)
  .transform.list         Size, Reverse, Sort, SortBy, Map, FlatMap, Flatten, Concat,
                          GroupBy, Enumerate, Zip, Rotate, Broadcast
  .transform.dom          ParseHtml, CssSelect, Text, OwnText, Attr, NthChild, Children,
                          Parent, OuterHtml, SpanExpand
  .transform.json         ParseJson, ParseXml, ParseLua, Path, Field,
                          AsString/Int/Long/Double/Boolean, Stringify, Deserialize,
                          ObjectBuild, Entries, JoinByKey, KeyLookup, ResolveAncestor
  .transform.encoding     Base64Encode/Decode, UrlEncode/Decode, HtmlDecode, JsonUnescape
  .predicate.string       Contains, Matches, StartsWith, EndsWith, Equals, NonEmpty
  .predicate.numeric      Int/Long/Double x {GreaterThan, LessThan, InRange}
  .predicate.dom          TextContains, TextMatches, HasAttr, TagEquals
  .predicate.json         HasField, FieldEquals
  .predicate.common       NotNull, Not, And, Or, Compare (+ CompareOperator)
  .terminal.collect       First, Last, Nth, List, SubList, Set, Join, Map,
                          JsonObjectFromEntries
  .terminal.sum           Count, SumInt, SumLong, SumDouble
  .terminal.average       AverageInt, AverageLong, AverageDouble
  .terminal.minmax        Min, Max, MinBy, MaxBy
  .terminal.match         AnyMatch, AllMatch, NoneMatch, FindFirst
```

## Stage catalog

DataTypes: `NONE`, `RAW_HTML`, `RAW_XML`, `RAW_JSON`, `STRING`, `INT`, `LONG`,
`FLOAT`, `DOUBLE`, `BOOLEAN`, `DOM_NODE`, `JSON_ELEMENT`, `JSON_OBJECT`, `JSON_ARRAY`,
`MAP_OUTPUT`, plus `List<X>` / `Set<X>` constructors and any type a host adds with
`DataTypes.register`.

Every kind below is the `@StageSpec(id = ...)` of one class, resolved on load by
`StageRegistry.byId`. The join, lookup, ancestor, group, enumerate, zip and broadcast stages
never mutate the rows they are given: what they emit is copied.

### Sources

| Kind                  | Input | Output                   | Notes                                                                 |
|-----------------------|-------|--------------------------|-----------------------------------------------------------------------|
| `SOURCE_URL`          | NONE  | `RAW_*` / `STRING`       | `ctx.fetcher()`; optional `maxBodyBytes`; any 4xx or 5xx, or a `ctx.fetchGuard()` refusal, fails the run (see [Fetching](#fetching)) |
| `SOURCE_LITERAL`      | NONE  | `T`                      | literal value parsed from config                                      |
| `SOURCE_LITERAL_LIST` | NONE  | `List<T>`                | literal list from JSON array                                          |
| `SOURCE_EMBED`        | NONE  | declared at construction | resolves saved pipeline by id                                         |

### Parse / DOM / JSON / encoding transforms

| Kind                          | Input               | Output                  | Notes                                                                                   |
|-------------------------------|---------------------|-------------------------|-----------------------------------------------------------------------------------------|
| `PARSE_HTML`                  | `RAW_HTML`          | `DOM_NODE`              |                                                                                         |
| `PARSE_XML`                   | `RAW_XML`           | `JSON_ELEMENT`          |                                                                                         |
| `PARSE_JSON`                  | `RAW_JSON`          | `JSON_ELEMENT`          |                                                                                         |
| `PARSE_LUA`                   | `STRING`            | `JSON_ELEMENT`          | a Lua 5.1 data module: `local`s, then `return` of a literal table; anything computed throws with its line and column |
| `TRANSFORM_CSS_SELECT`        | `DOM_NODE`          | `List<DOM_NODE>`        |                                                                                         |
| `TRANSFORM_DOM_TEXT`          | `DOM_NODE`          | `STRING`                |                                                                                         |
| `TRANSFORM_DOM_ATTR`          | `DOM_NODE`          | `STRING`                |                                                                                         |
| `TRANSFORM_DOM_NTH_CHILD`     | `DOM_NODE`          | `DOM_NODE`              |                                                                                         |
| `TRANSFORM_DOM_CHILDREN`      | `DOM_NODE`          | `List<DOM_NODE>`        |                                                                                         |
| `TRANSFORM_DOM_PARENT`        | `DOM_NODE`          | `DOM_NODE`              |                                                                                         |
| `TRANSFORM_DOM_OUTER_HTML`    | `DOM_NODE`          | `STRING`                |                                                                                         |
| `TRANSFORM_DOM_OWN_TEXT`      | `DOM_NODE`          | `STRING`                |                                                                                         |
| `TRANSFORM_DOM_SPAN_EXPAND`   | `DOM_NODE`          | `List<List<DOM_NODE>>`  | a `table` as a grid, each cell at every position its `rowspan` / `colspan` covers; optional `rowSelector`; a non-table rejects |
| `TRANSFORM_JSON_PATH`         | `JSON_ELEMENT`      | `JSON_ELEMENT`          |                                                                                         |
| `TRANSFORM_JSON_FIELD`        | `JSON_OBJECT`       | `JSON_ELEMENT`          |                                                                                         |
| `TRANSFORM_JSON_AS_STRING`    | `JSON_ELEMENT`      | `STRING`                |                                                                                         |
| `TRANSFORM_JSON_AS_INT`       | `JSON_ELEMENT`      | `INT`                   |                                                                                         |
| `TRANSFORM_JSON_AS_LONG`      | `JSON_ELEMENT`      | `LONG`                  |                                                                                         |
| `TRANSFORM_JSON_AS_DOUBLE`    | `JSON_ELEMENT`      | `DOUBLE`                |                                                                                         |
| `TRANSFORM_JSON_AS_BOOLEAN`   | `JSON_ELEMENT`      | `BOOLEAN`               |                                                                                         |
| `TRANSFORM_JSON_STRINGIFY`    | `JSON_ELEMENT`      | `STRING`                | compact JSON, no HTML escaping                                                          |
| `TRANSFORM_JSON_DESERIALIZE`  | `JSON_*`            | Basic `T` or `List<X>`  | Gson into a Basic type; for `List<X>` (Basic `X`) a JSON array becomes a list in array order, a null or unconvertible element is dropped, and a non-array rejects |
| `TRANSFORM_JSON_OBJECT_BUILD` | `I`                 | `JSON_OBJECT`           | one field per typed named body, in declared order; a `null` result omits its field      |
| `TRANSFORM_JSON_ENTRIES`      | `JSON_OBJECT`       | `List<JSON_OBJECT>`     | one `{key, value}` object per entry, in document order; `inline` spreads an object value's fields; optional `keyField` / `valueField` |
| `TRANSFORM_JOIN_BY_KEY`       | `List<JSON_OBJECT>` | `List<JSON_OBJECT>`     | merges a second document's rows by key (PIPELINE operand `right`); `INNER` / `LEFT` / `FULL`; a right value fills only an empty left cell; optional `columns` |
| `TRANSFORM_KEY_LOOKUP`        | `STRING`            | `JSON_ELEMENT`          | `valueField` of the row whose `keyField` is the input, in a second document (PIPELINE operand `table`); a miss rejects |
| `TRANSFORM_RESOLVE_ANCESTOR`  | `List<JSON_OBJECT>` | `List<JSON_OBJECT>`     | follows each row's `parentField` to its root and writes the root's key or `valueField` into `outputField`; optional `depthField`; a cycle writes neither |
| `TRANSFORM_BASE64_ENCODE`     | `STRING`            | `STRING`                |                                                                                         |
| `TRANSFORM_BASE64_DECODE`     | `STRING`            | `STRING`                |                                                                                         |
| `TRANSFORM_URL_ENCODE`        | `STRING`            | `STRING`                |                                                                                         |
| `TRANSFORM_URL_DECODE`        | `STRING`            | `STRING`                |                                                                                         |
| `TRANSFORM_HTML_DECODE`       | `STRING`            | `STRING`                | HTML character references, decoded once, as a parser decodes text                      |
| `TRANSFORM_JSON_UNESCAPE`     | `STRING`            | `STRING`                | JSON string escapes, surrogate pairs joined; a malformed escape rejects                 |

### String transforms

| Kind                       | Input    | Output             | Notes                                                                    |
|----------------------------|----------|--------------------|--------------------------------------------------------------------------|
| `TRANSFORM_REGEX_EXTRACT`  | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_TRIM`           | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_REPLACE`        | `STRING` | `STRING`           | regex, fixed replacement                                                 |
| `TRANSFORM_REPLACE_MATCH`  | `STRING` | `STRING`           | each match's capture `group` rewritten through a body (`STRING -> STRING`), inserted literally; a `null` result keeps the match |
| `TRANSFORM_SPLIT`          | `STRING` | `List<STRING>`     |                                                                          |
| `TRANSFORM_LOWERCASE`      | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_UPPERCASE`      | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_CAMEL_CASE`     | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_PASCAL_CASE`    | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_SNAKE_CASE`     | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_TITLE_CASE`     | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_STRING_LENGTH`  | `STRING` | `INT`              |                                                                          |
| `TRANSFORM_PREFIX`         | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_SUFFIX`         | `STRING` | `STRING`           |                                                                          |
| `TRANSFORM_VALUE_MAP`      | `STRING` | `STRING`           | exact lookup in a `STRING_MAP` `table`; a miss takes `defaultValue`, rejects under `strict`, or passes through |
| `TRANSFORM_RANGE_EXPAND`   | `STRING` | `List<INT>`        | `"1-15"` to `[1, ..., 15]`; optional `regex`, `step`, `maxSize` (1000)   |
| `TRANSFORM_FETCH`          | `STRING` | `RAW_*` / `STRING` | fetches the URL the input names, or `urlTemplate` with `{}` replaced by it; a `ClientError` other than 408 and 429 rejects, any other failure throws; optional `maxBodyBytes` (see [Fetching](#fetching)) |

### List transforms

| Kind                       | Input               | Output              | Notes                                                                   |
|----------------------------|---------------------|---------------------|-------------------------------------------------------------------------|
| `TRANSFORM_LIST_LENGTH`    | `List<T>`           | `INT`               |                                                                         |
| `TRANSFORM_REVERSE`        | `List<T>`           | `List<T>`           |                                                                         |
| `TRANSFORM_SORT`           | `List<T>`           | `List<T>`           | natural ordering, asc/desc                                              |
| `TRANSFORM_SORT_BY`        | `List<T>`           | `List<T>`           | key-extractor body (`T -> K`)                                           |
| `TRANSFORM_MAP`            | `List<X>`           | `List<Y>`           | body `X -> Y` per element; a `null` result drops the element            |
| `TRANSFORM_FLAT_MAP`       | `List<X>`           | `List<Y>`           | body `X -> List<Y>`, concatenated                                       |
| `TRANSFORM_FLATTEN`        | `List<List<T>>`     | `List<T>`           |                                                                         |
| `TRANSFORM_CONCAT`         | `List<T>`           | `List<T>`           | appends a second document's elements (PIPELINE operand `other`)         |
| `TRANSFORM_GROUP_BY`       | `List<JSON_OBJECT>` | `List<JSON_OBJECT>` | one row per `keyField` value, first-seen order; per-field `aggregates` (`STRING_MAP` of `FIRST`, `LAST`, `LIST`, `UNION`, `CONCAT`, `MAX`, `MIN`, `COUNT`, `MODE`), else first non-null |
| `TRANSFORM_ENUMERATE`      | `List<T>`           | `List<JSON_OBJECT>` | `{index, value}` per element; optional `start`, `indexKey`, `valueKey`  |
| `TRANSFORM_ZIP`            | `I`                 | `List<JSON_OBJECT>` | pairs two lists read out of one value (bodies `left`, `right`) by position; `SHORTEST` / `LONGEST` |
| `TRANSFORM_ROTATE`         | `I`                 | `List<T>`           | a list read out of the value (body `list`) rotated by an offset read out of it (body `offset`) |
| `TRANSFORM_BROADCAST`      | `I`                 | `List<JSON_OBJECT>` | one parent value (body `parent`) carried onto every child of a list (body `children`) |

### Primitive / numeric transforms

| Kind                                 | Input    | Output             | Notes                                                           |
|--------------------------------------|----------|--------------------|-----------------------------------------------------------------|
| `TRANSFORM_PARSE_INT`                | `STRING` | `INT`              |                                                                 |
| `TRANSFORM_PARSE_LONG`               | `STRING` | `LONG`             |                                                                 |
| `TRANSFORM_PARSE_FLOAT`              | `STRING` | `FLOAT`            |                                                                 |
| `TRANSFORM_PARSE_DOUBLE`             | `STRING` | `DOUBLE`           |                                                                 |
| `TRANSFORM_PARSE_BOOLEAN`            | `STRING` | `BOOLEAN`          |                                                                 |
| `TRANSFORM_PARSE_ROMAN`              | `STRING` | `INT`              | canonical numerals `I` to `MMMCMXCIX`, either case; anything else rejects |
| `TRANSFORM_ABS_INT`                  | `INT`    | `INT`              |                                                                 |
| `TRANSFORM_ABS_LONG`                 | `LONG`   | `LONG`             |                                                                 |
| `TRANSFORM_ABS_FLOAT`                | `FLOAT`  | `FLOAT`            |                                                                 |
| `TRANSFORM_ABS_DOUBLE`               | `DOUBLE` | `DOUBLE`           |                                                                 |
| `TRANSFORM_NEGATE_INT`               | `INT`    | `INT`              |                                                                 |
| `TRANSFORM_NEGATE_LONG`              | `LONG`   | `LONG`             |                                                                 |
| `TRANSFORM_NEGATE_FLOAT`             | `FLOAT`  | `FLOAT`            |                                                                 |
| `TRANSFORM_NEGATE_DOUBLE`            | `DOUBLE` | `DOUBLE`           |                                                                 |
| `TRANSFORM_ARITHMETIC_INT`           | `INT`    | `INT`              | `input OP operand`, `operand` an `INT`                          |
| `TRANSFORM_ARITHMETIC_LONG`          | `LONG`   | `LONG`             | `input OP operand`, `operand` a `LONG`                          |
| `TRANSFORM_ARITHMETIC_FLOAT`         | `FLOAT`  | `FLOAT`            | `input OP operand`, `operand` a `DOUBLE` narrowed to `float`    |
| `TRANSFORM_ARITHMETIC_DOUBLE`        | `DOUBLE` | `DOUBLE`           | `input OP operand`, `operand` a `DOUBLE`                        |
| `TRANSFORM_BINARY_ARITHMETIC_INT`    | `I`      | `INT`              | `left OP right`, both bodies `I -> INT`                         |
| `TRANSFORM_BINARY_ARITHMETIC_LONG`   | `I`      | `LONG`             | `left OP right`, both bodies `I -> LONG`                        |
| `TRANSFORM_BINARY_ARITHMETIC_FLOAT`  | `I`      | `FLOAT`            | `left OP right`, both bodies `I -> FLOAT`                       |
| `TRANSFORM_BINARY_ARITHMETIC_DOUBLE` | `I`      | `DOUBLE`           | `left OP right`, both bodies `I -> DOUBLE`                      |
| `TRANSFORM_ROUND_FLOAT`              | `FLOAT`  | `FLOAT`            | `scale` decimal places, optional `mode` (`RoundingMode`, `HALF_UP`) |
| `TRANSFORM_ROUND_DOUBLE`             | `DOUBLE` | `DOUBLE`           | `scale` decimal places, optional `mode` (`RoundingMode`, `HALF_UP`) |
| `TRANSFORM_CONSTANT`                 | `I`      | `T`                | every present input replaced by `value`, parsed as `SOURCE_LITERAL` parses it |
| `TRANSFORM_COALESCE`                 | `I`      | `O`                | first non-`null` of `body`, `fallback` body and `defaultValue`; optional `when` body guards `body` |
| `TRANSFORM_TO_STRING`                | `T`      | `STRING`           |                                                                 |
| `TRANSFORM_TO_RAW`                   | `STRING` | `RAW_*`            | retypes the string; parses nothing                              |
| `TRANSFORM_PEEK`                     | `T`      | `T`                | identity + `ctx.log()` side effect                              |
| `TRANSFORM_EXPECT`                   | `T`      | `T`                | identity while the predicate `body` (`T -> BOOLEAN`) yields `true`; `false` or `null` fails the run with `ExpectationFailedException` naming the `expectation` sentence (see [Expectations](#expectations)) |

`OP` is an `ArithmeticOperator` named by its constant: `ADD`, `SUBTRACT`, `MULTIPLY`, `DIVIDE`,
`MODULO`. `INT` and `LONG` compute through `Math.*Exact`, so an overflow throws
`ArithmeticException`, and `DIVIDE` truncates toward zero. `MODULO` is a floored remainder
(`Math.floorMod`) for every type. A zero divisor under `DIVIDE` or `MODULO`, or a `NaN` or
infinite `FLOAT` / `DOUBLE` result, rejects with `null`. A binary stage rejects when either body
yields `null`, and the right body does not run once the left one has yielded `null`. A fraction
in an `INT` or `LONG` operand, or a `FLOAT` operand that is not finite or narrows to zero, fails
the load.

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
| `FILTER_DISTINCT_BY`          | `List<T>`               | `List<T>`               | key body (`T -> K`); first per key, last with `keepLast`; a `null` key drops |
| `FILTER_TAKE_WHILE`           | `List<T>`               | `List<T>`               | predicate body (`T -> BOOLEAN`)|
| `FILTER_DROP_WHILE`           | `List<T>`               | `List<T>`               | predicate body (`T -> BOOLEAN`)|
| `FILTER_WHERE`                | `List<T>`               | `List<T>`               | predicate body (`T -> BOOLEAN`) over every element; keeps each it yields `true` for, in order - `false`, `null` and a `null` element drop |

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
| `PREDICATE_COMPARE`               | `I`            | `BOOLEAN`  |

`AND` / `OR` carry a `SUB_PIPELINES_MAP` of named predicate bodies and short-circuit.

`COMPARE` tests `left OP right` over two values of one input, each read by its own body
(`left`, `right`: `I -> V`). `valueType` is one of `INT`, `LONG`, `FLOAT`, `DOUBLE`, `STRING` or
`BOOLEAN`, and `operator` a `CompareOperator` named by its constant: `EQUALS`, `NOT_EQUALS`,
`LESS_THAN`, `LESS_OR_EQUAL`, `GREATER_THAN`, `GREATER_OR_EQUAL`. Numbers compare numerically
(`-0.0` equals `0.0`), strings by `String.compareTo` (case-sensitive), and booleans by equality
alone, so an ordering operator over `BOOLEAN` fails the load. Either body yielding `null`, or a
`NaN` on either side, yields `null`, and the right body does not run once the left one has yielded
`null` - so a `FILTER_WHERE` over it drops an element either side cannot be read from:

```json
{
  "kind": "FILTER_WHERE",
  "elementType": "JSON_OBJECT",
  "body": [{
    "kind": "PREDICATE_COMPARE",
    "inputType": "JSON_OBJECT",
    "valueType": "INT",
    "operator": "GREATER_OR_EQUAL",
    "left": [{"kind": "TRANSFORM_JSON_FIELD", "fieldName": "tier"}, {"kind": "TRANSFORM_JSON_AS_INT"}],
    "right": [{"kind": "TRANSFORM_CONSTANT", "inputType": "JSON_OBJECT", "outputType": "INT", "value": "3"}]
  }]
}
```

### Terminals

| Kind                       | Input            | Output                 | Notes                              |
|----------------------------|------------------|------------------------|------------------------------------|
| `COLLECT_FIRST`            | `List<T>`        | `T`                    |                                    |
| `COLLECT_LAST`             | `List<T>`        | `T`                    |                                    |
| `COLLECT_NTH`              | `List<T>`        | `T`                    | element at `index`; past the end rejects |
| `COLLECT_LIST`             | `List<T>`        | `List<T>`              | identity terminal marker           |
| `COLLECT_SUB_LIST`         | `List<T>`        | `List<T>`              | `[from, to)`, `to` optional        |
| `COLLECT_SET`              | `List<T>`        | `Set<T>`               |                                    |
| `COLLECT_JOIN`             | `List<STRING>`   | `STRING`               |                                    |
| `COLLECT_JSON_OBJECT_FROM_ENTRIES` | `List<JSON_OBJECT>` | `JSON_OBJECT`  | merges `{key, value}` entries; the inverse of `TRANSFORM_JSON_ENTRIES` |
| `COLLECT_MAP`              | `I`              | `MAP_OUTPUT`           | named bodies over one input, in declared order; a `null` result omits its name |
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

## Configuration slots

A stage's configuration is read off its `of(...)` factory: each `@Configurable` parameter is one
slot, keyed on the wire by the parameter's name (or `@Configurable(name = ...)`), and the
parameter's Java type picks the slot's `FieldSpec.Type`, which fixes its wire form.

| `FieldSpec.Type`          | Factory parameter                | Wire form                                                          |
|---------------------------|----------------------------------|--------------------------------------------------------------------|
| `STRING`                  | `String`                         | string                                                             |
| `INT`                     | `int` / `Integer`                | number or numeric text; a fraction or a value past the `int` range fails the load  |
| `LONG`                    | `long` / `Long`                  | number or numeric text; a fraction or a value past the `long` range fails the load |
| `DOUBLE`                  | `double` / `Double`              | number or numeric text                                             |
| `BOOLEAN`                 | `boolean` / `Boolean`            | boolean, or the text `true` / `false` in any case                  |
| `DATA_TYPE`               | `DataType<?>`                    | type label, such as `"List<JSON_OBJECT>"`                          |
| `SUB_PIPELINE`            | `List<? extends Stage<?, ?>>` / `Chain` | array of stages with no source - a body run against a value |
| `SUB_PIPELINES_MAP`       | `NamedChains` / `Map<String, List<...>>` | object of name to stage array                              |
| `TYPED_SUB_PIPELINES_MAP` | `Map<String, TypedChain<?>>`     | object of name to `{"outputType": ..., "chain": [...]}`            |
| `PIPELINE`                | `DataPipeline<?>`                | array of stages whose stage 0 is a source - a whole pipeline file  |
| `STRING_MAP`              | `Map<String, String>`            | object of strings, kept in document order                          |

A `STRING_MAP` value that is a number or boolean reads as its text; a JSON null, object or
array fails the load. `TRANSFORM_VALUE_MAP`'s `table` and `TRANSFORM_GROUP_BY`'s `aggregates`
are `STRING_MAP` slots:

```json
{"kind": "TRANSFORM_VALUE_MAP", "table": {"PINK": "LIGHT_PURPLE", "GREY": "GRAY"}, "strict": true}
```

A `PIPELINE` slot is an operand - see [Reading a second document](#reading-a-second-document).
Every key a stage carries on the wire must be one of its slots, and every required slot must be
present - see [Loading](#loading).

## Persisting a pipeline

```java
String json = PipelineGson.toJson(pipeline);   // round-trip-safe wire format
DataPipeline<?> rebuilt = PipelineGson.fromJson(json);
```

The wire format is a top-level JSON array of stage descriptors, each carrying a `"kind"`
discriminator plus its configuration fields. A stage's factory runs on load, so a body or
operand of the wrong type, or a config value the factory refuses, fails `fromJson` rather than
the first run.

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
breadcrumb of every active id, whether the embed is stage 0 of the pipeline or of an operand.

## Reading a second document

A pipeline reads one document at stage 0. A stage that needs a second one - rows to join, a
table to look keys up in, a list to append - carries it as a **pipeline operand**: a `PIPELINE`
slot holding a whole sourced pipeline. `TRANSFORM_JOIN_BY_KEY` (`right`),
`TRANSFORM_KEY_LOOKUP` (`table`) and `TRANSFORM_CONCAT` (`other`) take one.

On the wire the operand is a stage array in the shape of a pipeline file. This joins each
item row to its price row:

```json
[
  {"kind": "SOURCE_URL", "outputType": "RAW_JSON", "url": "https://example.com/items.json"},
  {"kind": "PARSE_JSON"},
  {"kind": "TRANSFORM_JSON_DESERIALIZE", "inputType": "JSON_ELEMENT", "outputType": "List<JSON_OBJECT>"},
  {
    "kind": "TRANSFORM_JOIN_BY_KEY",
    "leftKey": "id",
    "rightKey": "itemId",
    "mode": "LEFT",
    "columns": "price",
    "right": [
      {"kind": "SOURCE_URL", "outputType": "RAW_JSON", "url": "https://example.com/prices.json"},
      {"kind": "PARSE_JSON"},
      {"kind": "TRANSFORM_JSON_DESERIALIZE", "inputType": "JSON_ELEMENT", "outputType": "List<JSON_OBJECT>"}
    ]
  }
]
```

A saved pipeline serves as the operand through a single `SOURCE_EMBED`:
`"right": [{"kind": "SOURCE_EMBED", "embeddedPipelineId": "prices", "outputType": "List<JSON_OBJECT>"}]`.
In Java the operand is any built pipeline:

```java
DataPipeline<List<JsonObject>> prices = DataPipeline.builder()
    .source(UrlSource.of(DataTypes.RAW_JSON, "https://example.com/prices.json"))
    .stage(ParseJsonTransform.of())
    .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
    .build();

DataPipeline<List<JsonObject>> items = DataPipeline.builder()
    .source(UrlSource.of(DataTypes.RAW_JSON, "https://example.com/items.json"))
    .stage(ParseJsonTransform.of())
    .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
    .stage(JoinByKeyTransform.of("id", "itemId", "LEFT", "price", prices))
    .build();
```

- **Checked when the stage is built.** The owning factory validates the operand as a pipeline
  file is validated, and checks that its last stage produces the type the stage consumes
  (`DataPipeline.validate(DataType)`). A mismatch throws `IllegalArgumentException("Invalid
  <Stage> operand: ...")`, so a wire file with a mis-typed operand fails on load.
- **Evaluated at most once per run.** The stage reads the operand through
  `PipelineContext.evaluateOperand`, which runs it against the same context - fetcher, resolver,
  bag, tracer - the first time and holds the result, a `null` included. A lookup inside a
  `TRANSFORM_MAP` body over a thousand elements reads its table once. The memo is keyed by the
  operand instance, belongs to the context, and starts empty in every new `PipelineContext`, so
  build one context per run: a context reused for a later run answers it with the operand values
  the first run read.
- **Shared, so never mutated.** Every read in the run sees the same value; the stages that take
  an operand copy rows before changing them.
- **Cycles throw.** An operand whose evaluation reaches itself throws
  `IllegalStateException("Pipeline operand cycle detected; ...")`, and an embed inside an operand
  goes through the `SOURCE_EMBED` cycle guard. An evaluation that throws holds nothing.

## Fetching

Two stages go to the network, both through `ctx.fetcher()` - the context's `UrlFetcher`, with
its headers, rate limit and response cache.

- **`SOURCE_URL`** fetches one configured URL at stage 0. Any error status (`4xx` or `5xx`), a
  transport failure, a body past the cap or a request the local rate limit refuses throws a
  `UrlFetchException` and fails the run.
- **`TRANSFORM_FETCH`** fetches the URL its input names: the input itself, or `urlTemplate` with
  every `{}` replaced by the input, which is substituted as given (a page name that needs escaping
  passes through an encoding stage first). A client error the origin answers (`400` to `451`, or
  a `4xx` the client's `HttpStatus` has no constant for outside Nginx's `494-499`, raised as
  `UrlFetchException.ClientError`) rejects the element with `null`, so a `TRANSFORM_MAP` drops a
  page that does not exist. A `408` or `429` is the exception: a timeout or throttling says
  nothing about whether the page exists, so it throws its `ClientError` and fails the run rather
  than silently shortening the collection. Any other error status (a `5xx`, or an Nginx `444` or
  `494-499`), a transport failure, a body past the cap, a local rate-limit refusal, a blank input
  or an input that does not form a URI throws too, so a collection is never silently short a page
  because the server or the network failed.

Every body either stage hands on passes through the context's `FetchGuard` first - a body the
response cache replays included - with the URL the stage requested, the same when the fetcher
followed a redirect. The body of a status the fetch raises for never reaches it. A host installs
one once and every pipeline run against the context gets its checks, so a check specific to a
source (an error page an origin answers with a `200`, say) is not repeated in each pipeline file.
A guard refuses a body by throwing, and the throw fails the run: `TRANSFORM_FETCH` never turns a
refusal into a dropped element, whatever the exception. A context built without one carries
`FetchGuard.NOOP`, which accepts every body.

```java
PipelineContext ctx = PipelineContext.builder()
    .withFetcher(fetcher)
    .withFetchGuard(MyWikiGuard::check)   // host-supplied: void check(URI uri, String body)
    .build();
```

Both take an optional `maxBodyBytes` (`LONG`): the largest body the fetch accepts. Absent, the
fetch is held to the fetcher's configured cap (`UrlFetcherConfig`, 5 MiB by default); negative,
the load fails. A body past the cap throws `UrlFetchException.BodyCapExceeded`, a cached body
included. An error status raises its own exception whatever the size of its body, so a `404`
page larger than the cap still drops its `TRANSFORM_FETCH` element. One page per id, capped at
256 KiB:

```json
[
  {"kind": "SOURCE_LITERAL_LIST", "elementType": "STRING", "value": "[\"1001\", \"1002\", \"9999\"]"},
  {
    "kind": "TRANSFORM_MAP",
    "elementInputType": "STRING",
    "elementOutputType": "JSON_ELEMENT",
    "body": [
      {"kind": "TRANSFORM_FETCH", "outputType": "RAW_JSON", "urlTemplate": "https://api.example.com/items/{}.json", "maxBodyBytes": 262144},
      {"kind": "PARSE_JSON"}
    ]
  }
]
```

When `9999` answers `404`, the result holds the two pages that exist.

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
`SortBy` / `MinBy` / `MaxBy` / `DistinctBy`, `And` / `Or`, `COLLECT_MAP`, `ObjectBuild`,
`ReplaceMatch`, `Coalesce`, the binary arithmetic stages, `Zip`, `Rotate` and `Broadcast`) are
wrapped as a `chain.Chain` and validated the same way via `Chain.validate(seed, body, expected)`
when the stage is built, against the declared element-type-in and output-type-out. A pipeline
operand is validated as a whole pipeline against the type its stage consumes
(`DataPipeline.validate(DataType)`). Both checks run on the typed builder path and on load
alike.

### Expectations

A `TRANSFORM_EXPECT` states, in a sentence, what the value passing through it must be, and its
predicate body tests it on every run. The report lists each one before anything runs:
`ValidationReport.expectations()` holds every `TRANSFORM_EXPECT` in the pipeline, in walk order,
at any depth - a body, a named or typed body, a pipeline operand - each as an `Expectation` of the
top-level stage index, the path of the stage, the sentence and the type it tests. Expectations
never decide validity: a report with expectations and no issues `isValid()`.

The path follows the wire form: `#` and the top-level index, then per level of nesting a dot, the
slot's key, the branch name for named bodies (then `.chain` for a typed body) and the index in
that stage array. `#1` is stage 1, `#1.body[0]` the first stage of its `body`,
`#1.outputs.id.chain[1]` the second stage of the `id` output of a
`TRANSFORM_JSON_OBJECT_BUILD`, and `#1.right[0]` stage 0 of a `right` operand.

```java
for (ValidationReport.Expectation expectation : pipeline.validate().expectations())
    System.out.println(expectation.path() + ": " + expectation.text());
// #1.body[0]: each id is an item id
```

When a run reaches a value the body yields `false` or `null` for, it stops with
`ExpectationFailedException`: `Expectation 'each id is an item id' failed: body yielded 'false'
for input '...' of type 'STRING'`, the input cut to 120 characters. A `null` value passes through
without running the body, so an expectation cannot require a value to be present; state it on the
value that holds it instead - a body over each row that reads the row's `id` yields `null` on a row
with none, and that fails.

### Widening

Every one of these checks asks whether the type produced is assignable to the type expected -
`DataType.isAssignableTo` - rather than equal to it:

- `JSON_OBJECT` and `JSON_ARRAY` are assignable to `JSON_ELEMENT`, so `TRANSFORM_JSON_STRINGIFY`
  or `TRANSFORM_JSON_PATH` follows a stage that produces an object without a
  `TRANSFORM_JSON_DESERIALIZE` in between.
- A `List<X>` or `Set<X>` is assignable to a `List<Y>` or `Set<Y>` when `X` is assignable to
  `Y`: rows typed `List<JSON_OBJECT>` feed a `TRANSFORM_MAP` over `JSON_ELEMENT`, or a
  `TRANSFORM_CONCAT` operand onto a `List<JSON_ELEMENT>`. A pipeline never mutates a collection it
  is handed, so reading one as a collection of a wider element is safe.

Nothing is converted when the pipeline runs - a `JsonObject` is a `JsonElement` already. Nothing
else widens: `RAW_HTML` is not a `STRING`, and no numeric type is assignable to another.

### Loading

`PipelineGson.fromJson` reads every stage strictly, at any depth - bodies and operands included -
and each refusal is an `IllegalArgumentException`:

- **A key the stage does not declare fails the load.** Every key besides `kind` must be one of the
  stage's slots, so a misspelt key is caught rather than read as an absent optional one:
  `Stage 'TRANSFORM_SPLIT' does not declare key 'regx' (declared keys: [regex])`.
- **A required key that is absent fails the load**, and so does one holding JSON `null`:
  `Stage 'TRANSFORM_SPLIT' is missing required key 'regex'`.
- **JSON `null` on an optional key reads as the key being absent**, and is not written back.
- **An object that names a key twice fails the load**, since only one of the two values would be
  read: `Pipeline JSON repeats key 'regex' at '$[1].regex'`.
- **A value of the wrong JSON shape fails the load** - an object where a string belongs, say, or
  anything but a stage array for a body - and so does a stage entry that is not an object or has no
  `kind`, and a `TRANSFORM_JSON_OBJECT_BUILD` output holding a key besides `outputType` and
  `chain`.
- **A scalar that does not read as its slot's type fails the load** rather than reading as
  something else: a boolean slot takes a JSON boolean or the text `true` / `false`, so `"yes"` or
  `1` is refused instead of read as `false`, and a number slot takes a number or numeric text:
  `Field 'inline' holds '"yes"' but a boolean was expected`.
- **A stage factory's refusal reaches the caller as the exception the factory threw**, with its own
  message: `UrlSource maxBodyBytes must not be negative but got '-1'`.

## Status

v0.1, pre-release. The Stream-parity catalog (FlatMap family, terminals, match collectors,
predicates, comparators, literal sources) is complete, and so is the table-shaping set built on
it: joins, key lookups, concatenation and ancestor resolution across documents, group-by,
distinct-by, enumerate, zip, rotate and broadcast over rows, per-element fetch, per-type
arithmetic and rounding, constants and coalescing, filtering by a predicate body, comparing two
values of one element, stated expectations that fail a run, and Lua, HTML-entity, JSON-escape and
table-span decoding.

Open:

- **No sink stage.** A pipeline produces a value; writing it anywhere - a file, a table, a
  commit - is the host's job.
- **Async / reactive `Stage` execution** remains deferred. An operand or a per-element fetch runs
  on the calling thread.

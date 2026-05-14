package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.stage.terminal.Branch;
import dev.sbs.dataflow.stage.terminal.match.AllMatchCollect;
import dev.sbs.dataflow.stage.terminal.match.AnyMatchCollect;
import dev.sbs.dataflow.stage.terminal.average.AverageDoubleCollect;
import dev.sbs.dataflow.stage.terminal.average.AverageIntCollect;
import dev.sbs.dataflow.stage.terminal.average.AverageLongCollect;
import dev.sbs.dataflow.stage.terminal.sum.CountCollect;
import dev.sbs.dataflow.stage.terminal.match.FindFirstCollect;
import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.terminal.collect.JoinCollect;
import dev.sbs.dataflow.stage.terminal.collect.LastCollect;
import dev.sbs.dataflow.stage.terminal.collect.ListCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MaxByCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MaxCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MinByCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MinCollect;
import dev.sbs.dataflow.stage.terminal.match.NoneMatchCollect;
import dev.sbs.dataflow.stage.terminal.collect.SetCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumDoubleCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumIntCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumLongCollect;
import dev.sbs.dataflow.stage.source.EmbedSource;
import dev.sbs.dataflow.stage.filter.dom.DomHasAttrFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTagEqualsFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTextMatchesFilter;
import dev.sbs.dataflow.stage.filter.json.JsonFieldEqualsFilter;
import dev.sbs.dataflow.stage.filter.json.JsonHasFieldFilter;
import dev.sbs.dataflow.stage.filter.list.DistinctFilter;
import dev.sbs.dataflow.stage.filter.list.DropWhileFilter;
import dev.sbs.dataflow.stage.filter.list.IndexInRangeFilter;
import dev.sbs.dataflow.stage.filter.list.NotNullFilter;
import dev.sbs.dataflow.stage.filter.list.SkipFilter;
import dev.sbs.dataflow.stage.filter.list.TakeFilter;
import dev.sbs.dataflow.stage.filter.list.TakeWhileFilter;
import dev.sbs.dataflow.stage.filter.numeric.DoubleGreaterThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.DoubleInRangeFilter;
import dev.sbs.dataflow.stage.filter.numeric.DoubleLessThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.IntGreaterThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.IntInRangeFilter;
import dev.sbs.dataflow.stage.filter.numeric.IntLessThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.LongGreaterThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.LongInRangeFilter;
import dev.sbs.dataflow.stage.filter.numeric.LongLessThanFilter;
import dev.sbs.dataflow.stage.filter.string.StringContainsFilter;
import dev.sbs.dataflow.stage.filter.string.StringEndsWithFilter;
import dev.sbs.dataflow.stage.filter.string.StringEqualsFilter;
import dev.sbs.dataflow.stage.filter.string.StringMatchesFilter;
import dev.sbs.dataflow.stage.filter.string.StringNonEmptyFilter;
import dev.sbs.dataflow.stage.filter.string.StringStartsWithFilter;
import dev.sbs.dataflow.stage.predicate.common.AndPredicate;
import dev.sbs.dataflow.stage.predicate.common.NotNullPredicate;
import dev.sbs.dataflow.stage.predicate.common.NotPredicate;
import dev.sbs.dataflow.stage.predicate.common.OrPredicate;
import dev.sbs.dataflow.stage.predicate.dom.DomHasAttrPredicate;
import dev.sbs.dataflow.stage.predicate.dom.DomTagEqualsPredicate;
import dev.sbs.dataflow.stage.predicate.dom.DomTextContainsPredicate;
import dev.sbs.dataflow.stage.predicate.dom.DomTextMatchesPredicate;
import dev.sbs.dataflow.stage.predicate.json.JsonFieldEqualsPredicate;
import dev.sbs.dataflow.stage.predicate.json.JsonHasFieldPredicate;
import dev.sbs.dataflow.stage.predicate.numeric.DoubleGreaterThanPredicate;
import dev.sbs.dataflow.stage.predicate.numeric.DoubleInRangePredicate;
import dev.sbs.dataflow.stage.predicate.numeric.DoubleLessThanPredicate;
import dev.sbs.dataflow.stage.predicate.numeric.IntGreaterThanPredicate;
import dev.sbs.dataflow.stage.predicate.numeric.IntInRangePredicate;
import dev.sbs.dataflow.stage.predicate.numeric.IntLessThanPredicate;
import dev.sbs.dataflow.stage.predicate.numeric.LongGreaterThanPredicate;
import dev.sbs.dataflow.stage.predicate.numeric.LongInRangePredicate;
import dev.sbs.dataflow.stage.predicate.numeric.LongLessThanPredicate;
import dev.sbs.dataflow.stage.predicate.string.StringContainsPredicate;
import dev.sbs.dataflow.stage.predicate.string.StringEndsWithPredicate;
import dev.sbs.dataflow.stage.predicate.string.StringEqualsPredicate;
import dev.sbs.dataflow.stage.predicate.string.StringMatchesPredicate;
import dev.sbs.dataflow.stage.predicate.string.StringNonEmptyPredicate;
import dev.sbs.dataflow.stage.predicate.string.StringStartsWithPredicate;
import dev.sbs.dataflow.stage.source.OfListSource;
import dev.sbs.dataflow.stage.source.OfSource;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.DomChildrenTransform;
import dev.sbs.dataflow.stage.transform.dom.DomOuterHtmlTransform;
import dev.sbs.dataflow.stage.transform.dom.DomOwnTextTransform;
import dev.sbs.dataflow.stage.transform.dom.DomParentTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeAttrTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.encoding.Base64DecodeTransform;
import dev.sbs.dataflow.stage.transform.encoding.Base64EncodeTransform;
import dev.sbs.dataflow.stage.transform.encoding.UrlDecodeTransform;
import dev.sbs.dataflow.stage.transform.encoding.UrlEncodeTransform;
import dev.sbs.dataflow.stage.transform.json.*;
import dev.sbs.dataflow.stage.transform.list.FlatMapTransform;
import dev.sbs.dataflow.stage.transform.list.FlattenTransform;
import dev.sbs.dataflow.stage.transform.list.ListLengthTransform;
import dev.sbs.dataflow.stage.transform.list.MapTransform;
import dev.sbs.dataflow.stage.transform.list.ReverseTransform;
import dev.sbs.dataflow.stage.transform.list.SortByTransform;
import dev.sbs.dataflow.stage.transform.list.SortTransform;
import dev.sbs.dataflow.stage.transform.primitive.PeekTransform;
import dev.sbs.dataflow.stage.transform.primitive.*;
import dev.sbs.dataflow.stage.transform.string.LowerCaseTransform;
import dev.sbs.dataflow.stage.transform.string.PrefixTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.string.ReplaceTransform;
import dev.sbs.dataflow.stage.transform.string.SplitTransform;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import dev.sbs.dataflow.stage.transform.string.SuffixTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import dev.sbs.dataflow.stage.transform.string.UpperCaseTransform;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Stable wire-format discriminator for a {@link Stage} kind.
 * <p>
 * Each enum constant carries everything the framework needs about a stage: display name,
 * type-signature description, coarse {@link StageCategory} grouping, configuration
 * {@link FieldSpec schema}, and the {@link Function factory} that builds a fresh stage
 * instance from a populated {@link StageConfig}.
 * <p>
 * Stages whose serde lands in v2 (currently only {@code TRANSFORM_MAP}) carry a {@code null}
 * factory; any attempt to deserialise them fails fast.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public enum StageKind {

    /* ---- Source ---- */
    SOURCE_EMBED(
        "Embed pipeline",
        "() -> O",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("embeddedPipelineId", FieldType.STRING, "Saved pipeline id", "wiki_dmg"),
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "INT")
        ),
        cfg -> EmbedSource.of(cfg.getString("embeddedPipelineId"), cfg.getDataType("outputType"))
    ),

    SOURCE_OF(
        "Of (literal)",
        "() -> T",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "STRING"),
            new FieldSpec("value", FieldType.STRING, "Value", "literal")
        ),
        OfSource::fromConfig
    ),

    SOURCE_OF_LIST(
        "OfList (JSON array)",
        "() -> List<T>",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("value", FieldType.STRING, "JSON array", "[\"a\",\"b\"]")
        ),
        OfListSource::fromConfig
    ),

    SOURCE_PASTE(
        "Paste Source",
        "() -> RAW_*",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("body", FieldType.STRING, "Body", "<inline content>"),
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "RAW_HTML / RAW_XML / RAW_JSON / RAW_TEXT")
        ),
        PasteSource::fromConfig
    ),

    SOURCE_URL(
        "URL Source",
        "() -> RAW_*",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("url", FieldType.STRING, "URL", "https://example.com/page"),
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "RAW_HTML / RAW_XML / RAW_JSON / RAW_TEXT")
        ),
        UrlSource::fromConfig
    ),

    /* ---- Filter ---- */
    FILTER_DISTINCT(
        "Distinct",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> DistinctFilter.of(cfg.getDataType("elementType"))
    ),

    FILTER_DOM_HAS_ATTR(
        "Has attribute",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(
            new FieldSpec("attributeName", FieldType.STRING, "Attribute name", "class"),
            new FieldSpec("expectedValue", FieldType.STRING, "Expected value (optional)", "primary")
        ),
        cfg -> {
            String name = cfg.getString("attributeName");
            String value = cfg.getString("expectedValue");
            return value.isEmpty()
                ? DomHasAttrFilter.of(name)
                : DomHasAttrFilter.of(name, value);
        }
    ),

    FILTER_DOM_TAG_EQUALS(
        "Tag equals",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(
            new FieldSpec("tagName", FieldType.STRING, "Tag", "a")
        ),
        cfg -> DomTagEqualsFilter.of(cfg.getString("tagName"))
    ),

    FILTER_DOM_TEXT_CONTAINS(
        "Text contains",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(new FieldSpec("needle", FieldType.STRING, "Text contains", "Dmg")),
        cfg -> DomTextContainsFilter.of(cfg.getString("needle"))
    ),

    FILTER_DOM_TEXT_MATCHES(
        "Text matches regex",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Text matches regex", "\\d+")
        ),
        cfg -> DomTextMatchesFilter.of(cfg.getString("regex"))
    ),

    FILTER_DOUBLE_GREATER_THAN(
        "Double >",
        "List<DOUBLE> -> List<DOUBLE>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.DOUBLE, "Threshold", "0.0")
        ),
        cfg -> DoubleGreaterThanFilter.of(cfg.getDouble("threshold"))
    ),

    FILTER_DOUBLE_IN_RANGE(
        "Double in [min, max]",
        "List<DOUBLE> -> List<DOUBLE>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.DOUBLE, "Min (inclusive)", "0.0"),
            new FieldSpec("max", FieldType.DOUBLE, "Max (inclusive)", "100.0")
        ),
        cfg -> DoubleInRangeFilter.of(cfg.getDouble("min"), cfg.getDouble("max"))
    ),

    FILTER_DOUBLE_LESS_THAN(
        "Double <",
        "List<DOUBLE> -> List<DOUBLE>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.DOUBLE, "Threshold", "0.0")
        ),
        cfg -> DoubleLessThanFilter.of(cfg.getDouble("threshold"))
    ),

    FILTER_DROP_WHILE(
        "DropWhile",
        "List<T> -> List<T> (body: T -> BOOLEAN)",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Predicate body", "")
        ),
        DropWhileFilter::fromConfig
    ),

    FILTER_INDEX_IN_RANGE(
        "Index in [from, to)",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("fromInclusive", FieldType.INT, "From (inclusive)", "0"),
            new FieldSpec("toExclusive", FieldType.INT, "To (exclusive)", "10")
        ),
        cfg -> IndexInRangeFilter.of(cfg.getDataType("elementType"), cfg.getInt("fromInclusive"), cfg.getInt("toExclusive"))
    ),

    FILTER_INT_GREATER_THAN(
        "Int >",
        "List<INT> -> List<INT>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.INT, "Threshold", "0")
        ),
        cfg -> IntGreaterThanFilter.of(cfg.getInt("threshold"))
    ),

    FILTER_INT_IN_RANGE(
        "Int in [min, max]",
        "List<INT> -> List<INT>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.INT, "Min (inclusive)", "0"),
            new FieldSpec("max", FieldType.INT, "Max (inclusive)", "100")
        ),
        cfg -> IntInRangeFilter.of(cfg.getInt("min"), cfg.getInt("max"))
    ),

    FILTER_INT_LESS_THAN(
        "Int <",
        "List<INT> -> List<INT>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.INT, "Threshold", "0")
        ),
        cfg -> IntLessThanFilter.of(cfg.getInt("threshold"))
    ),

    FILTER_JSON_FIELD_EQUALS(
        "Field equals",
        "List<JSON_OBJECT> -> List<JSON_OBJECT>",
        StageCategory.FILTER_JSON,
        List.of(
            new FieldSpec("fieldName", FieldType.STRING, "Field name", "rare"),
            new FieldSpec("expectedValue", FieldType.STRING, "Equals", "true")
        ),
        cfg -> JsonFieldEqualsFilter.of(cfg.getString("fieldName"), cfg.getString("expectedValue"))
    ),

    FILTER_JSON_HAS_FIELD(
        "Has field",
        "List<JSON_OBJECT> -> List<JSON_OBJECT>",
        StageCategory.FILTER_JSON,
        List.of(
            new FieldSpec("fieldName", FieldType.STRING, "Field name", "rare")
        ),
        cfg -> JsonHasFieldFilter.of(cfg.getString("fieldName"))
    ),

    FILTER_LONG_GREATER_THAN(
        "Long >",
        "List<LONG> -> List<LONG>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.LONG, "Threshold", "0")
        ),
        cfg -> LongGreaterThanFilter.of(cfg.getLong("threshold"))
    ),

    FILTER_LONG_IN_RANGE(
        "Long in [min, max]",
        "List<LONG> -> List<LONG>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.LONG, "Min (inclusive)", "0"),
            new FieldSpec("max", FieldType.LONG, "Max (inclusive)", "100")
        ),
        cfg -> LongInRangeFilter.of(cfg.getLong("min"), cfg.getLong("max"))
    ),

    FILTER_LONG_LESS_THAN(
        "Long <",
        "List<LONG> -> List<LONG>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.LONG, "Threshold", "0")
        ),
        cfg -> LongLessThanFilter.of(cfg.getLong("threshold"))
    ),

    FILTER_NOT_NULL(
        "Not null",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> NotNullFilter.of(cfg.getDataType("elementType"))
    ),

    FILTER_SKIP(
        "Skip first N",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("count", FieldType.INT, "Count", "10")
        ),
        cfg -> SkipFilter.of(cfg.getDataType("elementType"), cfg.getInt("count"))
    ),

    FILTER_STRING_CONTAINS(
        "Contains",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("needle", FieldType.STRING, "Contains", "foo")
        ),
        cfg -> StringContainsFilter.of(cfg.getString("needle"))
    ),

    FILTER_STRING_ENDS_WITH(
        "Ends with",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("suffix", FieldType.STRING, "Suffix", "bar")
        ),
        cfg -> StringEndsWithFilter.of(cfg.getString("suffix"))
    ),

    FILTER_STRING_EQUALS(
        "Equals",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("target", FieldType.STRING, "Equals", "foo")
        ),
        cfg -> StringEqualsFilter.of(cfg.getString("target"))
    ),

    FILTER_STRING_MATCHES(
        "Matches regex",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", "^foo")
        ),
        cfg -> StringMatchesFilter.of(cfg.getString("regex"))
    ),

    FILTER_STRING_NON_EMPTY(
        "Non-empty",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(),
        cfg -> StringNonEmptyFilter.of()
    ),

    FILTER_STRING_STARTS_WITH(
        "Starts with",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("prefix", FieldType.STRING, "Prefix", "foo")
        ),
        cfg -> StringStartsWithFilter.of(cfg.getString("prefix"))
    ),

    FILTER_TAKE(
        "Take first N",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("count", FieldType.INT, "Count", "10")
        ),
        cfg -> TakeFilter.of(cfg.getDataType("elementType"), cfg.getInt("count"))
    ),

    FILTER_TAKE_WHILE(
        "TakeWhile",
        "List<T> -> List<T> (body: T -> BOOLEAN)",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Predicate body", "")
        ),
        TakeWhileFilter::fromConfig
    ),

    /* ---- Transform ---- */
    PARSE_HTML(
        "Parse HTML",
        "RAW_HTML -> DOM_NODE",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> ParseHtmlTransform.of()
    ),

    PARSE_JSON(
        "Parse JSON",
        "RAW_JSON -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> ParseJsonTransform.of()
    ),

    PARSE_XML(
        "Parse XML",
        "RAW_XML -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> ParseXmlTransform.of()
    ),

    TRANSFORM_ABS_DOUBLE(
        "Abs double",
        "DOUBLE -> DOUBLE",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsDoubleTransform.of()
    ),

    TRANSFORM_ABS_FLOAT(
        "Abs float",
        "FLOAT -> FLOAT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsFloatTransform.of()
    ),

    TRANSFORM_ABS_INT(
        "Abs int",
        "INT -> INT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsIntTransform.of()
    ),

    TRANSFORM_ABS_LONG(
        "Abs long",
        "LONG -> LONG",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsLongTransform.of()
    ),

    TRANSFORM_BASE64_DECODE(
        "Base64 decode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> Base64DecodeTransform.of()
    ),

    TRANSFORM_BASE64_ENCODE(
        "Base64 encode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> Base64EncodeTransform.of()
    ),

    TRANSFORM_CSS_SELECT(
        "CSS select",
        "DOM_NODE -> List<DOM_NODE>",
        StageCategory.TRANSFORM_DOM,
        List.of(new FieldSpec("selector", FieldType.STRING, "Selector", "table.infobox tr")),
        cfg -> CssSelectTransform.of(cfg.getString("selector"))
    ),

    TRANSFORM_DOM_CHILDREN(
        "DOM children",
        "DOM_NODE -> List<DOM_NODE>",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomChildrenTransform.of()
    ),

    TRANSFORM_DOM_OUTER_HTML(
        "DOM outerHtml",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomOuterHtmlTransform.of()
    ),

    TRANSFORM_DOM_OWN_TEXT(
        "DOM ownText",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomOwnTextTransform.of()
    ),

    TRANSFORM_DOM_PARENT(
        "DOM parent",
        "DOM_NODE -> DOM_NODE",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomParentTransform.of()
    ),

    TRANSFORM_FLATTEN(
        "Flatten",
        "List<List<T>> -> List<T>",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> FlattenTransform.of(cfg.getDataType("elementType"))
    ),

    TRANSFORM_FLAT_MAP(
        "FlatMap sub-pipeline",
        "List<X> -> List<Y> (body: X -> List<Y>)",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementInputType", FieldType.DATA_TYPE, "Input element type", "STRING"),
            new FieldSpec("elementOutputType", FieldType.DATA_TYPE, "Output element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Per-element body (yields List<Y>)", "")
        ),
        FlatMapTransform::fromConfig
    ),

    TRANSFORM_JSON_AS_BOOLEAN(
        "JSON as boolean",
        "JSON_ELEMENT -> BOOLEAN",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsBooleanTransform.of()
    ),

    TRANSFORM_JSON_AS_DOUBLE(
        "JSON as double",
        "JSON_ELEMENT -> DOUBLE",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsDoubleTransform.of()
    ),

    TRANSFORM_JSON_AS_INT(
        "JSON as int",
        "JSON_ELEMENT -> INT",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsIntTransform.of()
    ),

    TRANSFORM_JSON_AS_LONG(
        "JSON as long",
        "JSON_ELEMENT -> LONG",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsLongTransform.of()
    ),

    TRANSFORM_JSON_AS_STRING(
        "JSON as string",
        "JSON_ELEMENT -> STRING",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsStringTransform.of()
    ),

    TRANSFORM_JSON_FIELD(
        "JSON field",
        "JSON_OBJECT -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(new FieldSpec("fieldName", FieldType.STRING, "Field name", "stats")),
        cfg -> JsonFieldTransform.of(cfg.getString("fieldName"))
    ),

    TRANSFORM_JSON_PATH(
        "JSON path",
        "JSON_ELEMENT -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(new FieldSpec("path", FieldType.STRING, "Path", "stats.combat.dmg")),
        cfg -> JsonPathTransform.of(cfg.getString("path"))
    ),

    TRANSFORM_JSON_STRINGIFY(
        "JSON stringify",
        "JSON_ELEMENT -> STRING",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonStringifyTransform.of()
    ),

    TRANSFORM_LIST_LENGTH(
        "List length",
        "List<T> -> INT",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> ListLengthTransform.of(cfg.getDataType("elementType"))
    ),

    TRANSFORM_LOWERCASE(
        "Lowercase",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> LowerCaseTransform.of()
    ),

    TRANSFORM_MAP(
        "Map sub-pipeline",
        "List<X> -> List<Y>",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementInputType", FieldType.DATA_TYPE, "Input element type", "STRING"),
            new FieldSpec("elementOutputType", FieldType.DATA_TYPE, "Output element type", "INT"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Per-element body", "")
        ),
        MapTransform::fromConfig
    ),

    TRANSFORM_NEGATE_DOUBLE(
        "Negate double",
        "DOUBLE -> DOUBLE",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateDoubleTransform.of()
    ),

    TRANSFORM_NEGATE_FLOAT(
        "Negate float",
        "FLOAT -> FLOAT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateFloatTransform.of()
    ),

    TRANSFORM_NEGATE_INT(
        "Negate int",
        "INT -> INT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateIntTransform.of()
    ),

    TRANSFORM_NEGATE_LONG(
        "Negate long",
        "LONG -> LONG",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateLongTransform.of()
    ),

    TRANSFORM_NODE_ATTR(
        "Node attribute",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(new FieldSpec("attributeName", FieldType.STRING, "Attribute name", "href")),
        cfg -> NodeAttrTransform.of(cfg.getString("attributeName"))
    ),

    TRANSFORM_NODE_TEXT(
        "Node text",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> NodeTextTransform.of()
    ),

    TRANSFORM_NTH_CHILD(
        "Nth child",
        "DOM_NODE -> DOM_NODE",
        StageCategory.TRANSFORM_DOM,
        List.of(
            new FieldSpec("childSelector", FieldType.STRING, "Child selector", "td"),
            new FieldSpec("index", FieldType.INT, "Index (0-based)", "0")
        ),
        cfg -> NthChildTransform.of(cfg.getString("childSelector"), cfg.getInt("index"))
    ),

    TRANSFORM_PARSE_BOOLEAN(
        "Parse boolean",
        "STRING -> BOOLEAN",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseBooleanTransform.of()
    ),

    TRANSFORM_PARSE_DOUBLE(
        "Parse double",
        "STRING -> DOUBLE",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseDoubleTransform.of()
    ),

    TRANSFORM_PARSE_FLOAT(
        "Parse float",
        "STRING -> FLOAT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseFloatTransform.of()
    ),

    TRANSFORM_PARSE_INT(
        "Parse int",
        "STRING -> INT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseIntTransform.of()
    ),

    TRANSFORM_PARSE_LONG(
        "Parse long",
        "STRING -> LONG",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseLongTransform.of()
    ),

    TRANSFORM_PEEK(
        "Peek (log)",
        "T -> T",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(
            new FieldSpec("valueType", FieldType.DATA_TYPE, "Value type", "STRING"),
            new FieldSpec("label", FieldType.STRING, "Label", "stage")
        ),
        cfg -> PeekTransform.of(cfg.getDataType("valueType"), cfg.getString("label"))
    ),

    TRANSFORM_PREFIX(
        "Prefix",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("prefix", FieldType.STRING, "Prefix", ">>>")
        ),
        cfg -> PrefixTransform.of(cfg.getString("prefix"))
    ),

    TRANSFORM_REGEX_EXTRACT(
        "Regex extract",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", "\\d+"),
            new FieldSpec("group", FieldType.INT, "Capture group", "0")
        ),
        cfg -> RegexExtractTransform.of(cfg.getString("regex"), cfg.getInt("group"))
    ),

    TRANSFORM_REPLACE(
        "Replace regex",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", "\\s+"),
            new FieldSpec("replacement", FieldType.STRING, "Replacement", "_")
        ),
        cfg -> ReplaceTransform.of(cfg.getString("regex"), cfg.getString("replacement"))
    ),

    TRANSFORM_REVERSE(
        "Reverse list",
        "List<T> -> List<T>",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> ReverseTransform.of(cfg.getDataType("elementType"))
    ),

    TRANSFORM_SORT(
        "Sort list",
        "List<T> -> List<T>",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "INT"),
            new FieldSpec("ascending", FieldType.BOOLEAN, "Ascending", "true")
        ),
        SortTransform::fromConfig
    ),

    TRANSFORM_SORT_BY(
        "Sort by key",
        "List<T> -> List<T> (body: T -> K)",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("keyType", FieldType.DATA_TYPE, "Key type", "INT"),
            new FieldSpec("ascending", FieldType.BOOLEAN, "Ascending", "true"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Key extractor body", "")
        ),
        SortByTransform::fromConfig
    ),

    TRANSFORM_SPLIT(
        "Split on regex",
        "STRING -> List<STRING>",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", ",")
        ),
        cfg -> SplitTransform.of(cfg.getString("regex"))
    ),

    TRANSFORM_STRING_LENGTH(
        "String length",
        "STRING -> INT",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> StringLengthTransform.of()
    ),

    TRANSFORM_SUFFIX(
        "Suffix",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("suffix", FieldType.STRING, "Suffix", "<<<")
        ),
        cfg -> SuffixTransform.of(cfg.getString("suffix"))
    ),

    TRANSFORM_TO_STRING(
        "To string",
        "T -> STRING",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(
            new FieldSpec("inputType", FieldType.DATA_TYPE, "Input type", "INT")
        ),
        cfg -> ToStringTransform.of(cfg.getDataType("inputType"))
    ),

    TRANSFORM_TRIM(
        "Trim",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> TrimTransform.of()
    ),

    TRANSFORM_UPPERCASE(
        "Uppercase",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> UpperCaseTransform.of()
    ),

    TRANSFORM_URL_DECODE(
        "URL decode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> UrlDecodeTransform.of()
    ),

    TRANSFORM_URL_ENCODE(
        "URL encode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> UrlEncodeTransform.of()
    ),

    /* ---- Predicate ---- */
    PREDICATE_AND(
        "And",
        "T -> BOOLEAN (AND over N predicate bodies)",
        StageCategory.PREDICATE_COMMON,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("bodies", FieldType.SUB_PIPELINES_MAP, "Predicate bodies", "")
        ),
        AndPredicate::fromConfig
    ),

    PREDICATE_DOM_HAS_ATTR(
        "Has attribute",
        "DOM_NODE -> BOOLEAN",
        StageCategory.PREDICATE_DOM,
        List.of(
            new FieldSpec("attributeName", FieldType.STRING, "Attribute name", "class"),
            new FieldSpec("expectedValue", FieldType.STRING, "Expected value (optional)", "primary")
        ),
        DomHasAttrPredicate::fromConfig
    ),

    PREDICATE_DOM_TAG_EQUALS(
        "Tag equals",
        "DOM_NODE -> BOOLEAN",
        StageCategory.PREDICATE_DOM,
        List.of(new FieldSpec("tagName", FieldType.STRING, "Tag", "a")),
        DomTagEqualsPredicate::fromConfig
    ),

    PREDICATE_DOM_TEXT_CONTAINS(
        "Text contains",
        "DOM_NODE -> BOOLEAN",
        StageCategory.PREDICATE_DOM,
        List.of(new FieldSpec("needle", FieldType.STRING, "Text contains", "Dmg")),
        DomTextContainsPredicate::fromConfig
    ),

    PREDICATE_DOM_TEXT_MATCHES(
        "Text matches regex",
        "DOM_NODE -> BOOLEAN",
        StageCategory.PREDICATE_DOM,
        List.of(new FieldSpec("regex", FieldType.STRING, "Text matches regex", "\\d+")),
        DomTextMatchesPredicate::fromConfig
    ),

    PREDICATE_DOUBLE_GREATER_THAN(
        "Double >",
        "DOUBLE -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(new FieldSpec("threshold", FieldType.DOUBLE, "Threshold", "0.0")),
        DoubleGreaterThanPredicate::fromConfig
    ),

    PREDICATE_DOUBLE_IN_RANGE(
        "Double in [min, max]",
        "DOUBLE -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.DOUBLE, "Min (inclusive)", "0.0"),
            new FieldSpec("max", FieldType.DOUBLE, "Max (inclusive)", "100.0")
        ),
        DoubleInRangePredicate::fromConfig
    ),

    PREDICATE_DOUBLE_LESS_THAN(
        "Double <",
        "DOUBLE -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(new FieldSpec("threshold", FieldType.DOUBLE, "Threshold", "0.0")),
        DoubleLessThanPredicate::fromConfig
    ),

    PREDICATE_INT_GREATER_THAN(
        "Int >",
        "INT -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(new FieldSpec("threshold", FieldType.INT, "Threshold", "0")),
        IntGreaterThanPredicate::fromConfig
    ),

    PREDICATE_INT_IN_RANGE(
        "Int in [min, max]",
        "INT -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.INT, "Min (inclusive)", "0"),
            new FieldSpec("max", FieldType.INT, "Max (inclusive)", "100")
        ),
        IntInRangePredicate::fromConfig
    ),

    PREDICATE_INT_LESS_THAN(
        "Int <",
        "INT -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(new FieldSpec("threshold", FieldType.INT, "Threshold", "0")),
        IntLessThanPredicate::fromConfig
    ),

    PREDICATE_JSON_FIELD_EQUALS(
        "Field equals",
        "JSON_OBJECT -> BOOLEAN",
        StageCategory.PREDICATE_JSON,
        List.of(
            new FieldSpec("fieldName", FieldType.STRING, "Field name", "rare"),
            new FieldSpec("expectedValue", FieldType.STRING, "Equals", "true")
        ),
        JsonFieldEqualsPredicate::fromConfig
    ),

    PREDICATE_JSON_HAS_FIELD(
        "Has field",
        "JSON_OBJECT -> BOOLEAN",
        StageCategory.PREDICATE_JSON,
        List.of(new FieldSpec("fieldName", FieldType.STRING, "Field name", "rare")),
        JsonHasFieldPredicate::fromConfig
    ),

    PREDICATE_LONG_GREATER_THAN(
        "Long >",
        "LONG -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(new FieldSpec("threshold", FieldType.LONG, "Threshold", "0")),
        LongGreaterThanPredicate::fromConfig
    ),

    PREDICATE_LONG_IN_RANGE(
        "Long in [min, max]",
        "LONG -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.LONG, "Min (inclusive)", "0"),
            new FieldSpec("max", FieldType.LONG, "Max (inclusive)", "100")
        ),
        LongInRangePredicate::fromConfig
    ),

    PREDICATE_LONG_LESS_THAN(
        "Long <",
        "LONG -> BOOLEAN",
        StageCategory.PREDICATE_NUMERIC,
        List.of(new FieldSpec("threshold", FieldType.LONG, "Threshold", "0")),
        LongLessThanPredicate::fromConfig
    ),

    PREDICATE_NOT(
        "Not",
        "BOOLEAN -> BOOLEAN",
        StageCategory.PREDICATE_COMMON,
        List.of(),
        cfg -> NotPredicate.of()
    ),

    PREDICATE_NOT_NULL(
        "Not null",
        "T -> BOOLEAN",
        StageCategory.PREDICATE_COMMON,
        List.of(new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")),
        NotNullPredicate::fromConfig
    ),

    PREDICATE_OR(
        "Or",
        "T -> BOOLEAN (OR over N predicate bodies)",
        StageCategory.PREDICATE_COMMON,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("bodies", FieldType.SUB_PIPELINES_MAP, "Predicate bodies", "")
        ),
        OrPredicate::fromConfig
    ),

    PREDICATE_STRING_CONTAINS(
        "Contains",
        "STRING -> BOOLEAN",
        StageCategory.PREDICATE_STRING,
        List.of(new FieldSpec("needle", FieldType.STRING, "Contains", "foo")),
        StringContainsPredicate::fromConfig
    ),

    PREDICATE_STRING_ENDS_WITH(
        "Ends with",
        "STRING -> BOOLEAN",
        StageCategory.PREDICATE_STRING,
        List.of(new FieldSpec("suffix", FieldType.STRING, "Suffix", "bar")),
        StringEndsWithPredicate::fromConfig
    ),

    PREDICATE_STRING_EQUALS(
        "Equals",
        "STRING -> BOOLEAN",
        StageCategory.PREDICATE_STRING,
        List.of(new FieldSpec("target", FieldType.STRING, "Equals", "foo")),
        StringEqualsPredicate::fromConfig
    ),

    PREDICATE_STRING_MATCHES(
        "Matches regex",
        "STRING -> BOOLEAN",
        StageCategory.PREDICATE_STRING,
        List.of(new FieldSpec("regex", FieldType.STRING, "Regex", "^foo")),
        StringMatchesPredicate::fromConfig
    ),

    PREDICATE_STRING_NON_EMPTY(
        "Non-empty",
        "STRING -> BOOLEAN",
        StageCategory.PREDICATE_STRING,
        List.of(),
        cfg -> StringNonEmptyPredicate.of()
    ),

    PREDICATE_STRING_STARTS_WITH(
        "Starts with",
        "STRING -> BOOLEAN",
        StageCategory.PREDICATE_STRING,
        List.of(new FieldSpec("prefix", FieldType.STRING, "Prefix", "foo")),
        StringStartsWithPredicate::fromConfig
    ),

    /* ---- Collect ---- */
    COLLECT_ALL_MATCH(
        "AllMatch",
        "List<T> -> BOOLEAN (body: T -> BOOLEAN)",
        StageCategory.TERMINAL_MATCH,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Predicate body", "")
        ),
        AllMatchCollect::fromConfig
    ),

    COLLECT_ANY_MATCH(
        "AnyMatch",
        "List<T> -> BOOLEAN (body: T -> BOOLEAN)",
        StageCategory.TERMINAL_MATCH,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Predicate body", "")
        ),
        AnyMatchCollect::fromConfig
    ),

    COLLECT_AVERAGE_DOUBLE(
        "Average DOUBLE",
        "List<DOUBLE> -> DOUBLE",
        StageCategory.TERMINAL_AVERAGE,
        List.of(),
        cfg -> AverageDoubleCollect.of()
    ),

    COLLECT_AVERAGE_INT(
        "Average INT",
        "List<INT> -> DOUBLE",
        StageCategory.TERMINAL_AVERAGE,
        List.of(),
        cfg -> AverageIntCollect.of()
    ),

    COLLECT_AVERAGE_LONG(
        "Average LONG",
        "List<LONG> -> DOUBLE",
        StageCategory.TERMINAL_AVERAGE,
        List.of(),
        cfg -> AverageLongCollect.of()
    ),

    COLLECT_COUNT(
        "Count",
        "List<T> -> INT",
        StageCategory.TERMINAL_SUM,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> CountCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_FIND_FIRST(
        "FindFirst (predicate)",
        "List<T> -> T (body: T -> BOOLEAN)",
        StageCategory.TERMINAL_MATCH,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Predicate body", "")
        ),
        FindFirstCollect::fromConfig
    ),

    COLLECT_FIRST(
        "First",
        "List<T> -> T",
        StageCategory.TERMINAL_COLLECT,
        List.of(new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "DOM_NODE")),
        cfg -> FirstCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_JOIN(
        "Join",
        "List<STRING> -> STRING",
        StageCategory.TERMINAL_COLLECT,
        List.of(
            new FieldSpec("separator", FieldType.STRING, "Separator", ", ")
        ),
        cfg -> JoinCollect.of(cfg.getString("separator"))
    ),

    COLLECT_LAST(
        "Last",
        "List<T> -> T",
        StageCategory.TERMINAL_COLLECT,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> LastCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_LIST(
        "List",
        "List<T> -> List<T>",
        StageCategory.TERMINAL_COLLECT,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> ListCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_MAX(
        "Max",
        "List<T> -> T",
        StageCategory.TERMINAL_MINMAX,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "INT")
        ),
        MaxCollect::fromConfig
    ),

    COLLECT_MAX_BY(
        "MaxBy (key)",
        "List<T> -> T (body: T -> K)",
        StageCategory.TERMINAL_MINMAX,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("keyType", FieldType.DATA_TYPE, "Key type", "INT"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Key extractor body", "")
        ),
        MaxByCollect::fromConfig
    ),

    COLLECT_MIN(
        "Min",
        "List<T> -> T",
        StageCategory.TERMINAL_MINMAX,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "INT")
        ),
        MinCollect::fromConfig
    ),

    COLLECT_MIN_BY(
        "MinBy (key)",
        "List<T> -> T (body: T -> K)",
        StageCategory.TERMINAL_MINMAX,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("keyType", FieldType.DATA_TYPE, "Key type", "INT"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Key extractor body", "")
        ),
        MinByCollect::fromConfig
    ),

    COLLECT_NONE_MATCH(
        "NoneMatch",
        "List<T> -> BOOLEAN (body: T -> BOOLEAN)",
        StageCategory.TERMINAL_MATCH,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("body", FieldType.SUB_PIPELINE, "Predicate body", "")
        ),
        NoneMatchCollect::fromConfig
    ),

    COLLECT_SET(
        "Set",
        "List<T> -> Set<T>",
        StageCategory.TERMINAL_COLLECT,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> SetCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_SUM_DOUBLE(
        "Sum DOUBLE",
        "List<DOUBLE> -> DOUBLE",
        StageCategory.TERMINAL_SUM,
        List.of(),
        cfg -> SumDoubleCollect.of()
    ),

    COLLECT_SUM_INT(
        "Sum INT",
        "List<INT> -> INT",
        StageCategory.TERMINAL_SUM,
        List.of(),
        cfg -> SumIntCollect.of()
    ),

    COLLECT_SUM_LONG(
        "Sum LONG",
        "List<LONG> -> LONG",
        StageCategory.TERMINAL_SUM,
        List.of(),
        cfg -> SumLongCollect.of()
    ),

    /* ---- Compound ---- */
    BRANCH(
        "Branch",
        "I -> Map<String, Object>",
        StageCategory.BRANCH,
        List.of(
            new FieldSpec("inputType", FieldType.DATA_TYPE, "Input type", "STRING"),
            new FieldSpec("outputs", FieldType.SUB_PIPELINES_MAP, "Outputs", "")
        ),
        Branch::fromConfig
    );

    /** Cached snapshot of {@link #values()} reused by lookups to avoid the per-call defensive array clone. */
    private static final StageKind @NotNull [] CACHED_VALUES = values();

    private final @NotNull String displayName;
    private final @NotNull String description;
    private final @NotNull StageCategory category;
    private final @NotNull List<FieldSpec> schema;
    private final @Nullable Function<StageConfig, Stage<?, ?>> factory;

    /**
     * Returns every {@link StageKind} in {@code category}, in declaration order.
     *
     * @param category the category
     * @return the kinds in {@code category}, possibly empty
     */
    public static @NotNull List<StageKind> ofCategory(@NotNull StageCategory category) {
        return Arrays.stream(CACHED_VALUES)
            .filter(id -> id.category == category)
            .toList();
    }

    /**
     * Returns whether this kind needs a configuration modal (any non-empty schema).
     *
     * @return {@code true} when {@link #schema()} has at least one slot
     */
    public boolean requiresModal() {
        return !this.schema.isEmpty();
    }

}

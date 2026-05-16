package dev.sbs.dataflow.stage;

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
import dev.sbs.dataflow.stage.source.EmbedSource;
import dev.sbs.dataflow.stage.source.LiteralListSource;
import dev.sbs.dataflow.stage.source.LiteralSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.terminal.average.AverageDoubleCollect;
import dev.sbs.dataflow.stage.terminal.average.AverageIntCollect;
import dev.sbs.dataflow.stage.terminal.average.AverageLongCollect;
import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.terminal.collect.JoinCollect;
import dev.sbs.dataflow.stage.terminal.collect.JsonObjectFromEntriesCollect;
import dev.sbs.dataflow.stage.terminal.collect.LastCollect;
import dev.sbs.dataflow.stage.terminal.collect.ListCollect;
import dev.sbs.dataflow.stage.terminal.collect.MapCollect;
import dev.sbs.dataflow.stage.terminal.collect.SetCollect;
import dev.sbs.dataflow.stage.terminal.match.AllMatchCollect;
import dev.sbs.dataflow.stage.terminal.match.AnyMatchCollect;
import dev.sbs.dataflow.stage.terminal.match.FindFirstCollect;
import dev.sbs.dataflow.stage.terminal.match.NoneMatchCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MaxByCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MaxCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MinByCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MinCollect;
import dev.sbs.dataflow.stage.terminal.sum.CountCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumDoubleCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumIntCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumLongCollect;
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
import dev.sbs.dataflow.stage.transform.json.GsonDeserializeTransform;
import dev.sbs.dataflow.stage.transform.json.JsonAsBooleanTransform;
import dev.sbs.dataflow.stage.transform.json.JsonAsDoubleTransform;
import dev.sbs.dataflow.stage.transform.json.JsonAsIntTransform;
import dev.sbs.dataflow.stage.transform.json.JsonAsLongTransform;
import dev.sbs.dataflow.stage.transform.json.JsonAsStringTransform;
import dev.sbs.dataflow.stage.transform.json.JsonFieldTransform;
import dev.sbs.dataflow.stage.transform.json.JsonObjectBuildTransform;
import dev.sbs.dataflow.stage.transform.json.JsonPathTransform;
import dev.sbs.dataflow.stage.transform.json.JsonStringifyTransform;
import dev.sbs.dataflow.stage.transform.json.ParseJsonTransform;
import dev.sbs.dataflow.stage.transform.json.ParseXmlTransform;
import dev.sbs.dataflow.stage.transform.list.FlatMapTransform;
import dev.sbs.dataflow.stage.transform.list.FlattenTransform;
import dev.sbs.dataflow.stage.transform.list.ListLengthTransform;
import dev.sbs.dataflow.stage.transform.list.MapTransform;
import dev.sbs.dataflow.stage.transform.list.ReverseTransform;
import dev.sbs.dataflow.stage.transform.list.SortByTransform;
import dev.sbs.dataflow.stage.transform.list.SortTransform;
import dev.sbs.dataflow.stage.transform.primitive.AbsDoubleTransform;
import dev.sbs.dataflow.stage.transform.primitive.AbsFloatTransform;
import dev.sbs.dataflow.stage.transform.primitive.AbsIntTransform;
import dev.sbs.dataflow.stage.transform.primitive.AbsLongTransform;
import dev.sbs.dataflow.stage.transform.primitive.NegateDoubleTransform;
import dev.sbs.dataflow.stage.transform.primitive.NegateFloatTransform;
import dev.sbs.dataflow.stage.transform.primitive.NegateIntTransform;
import dev.sbs.dataflow.stage.transform.primitive.NegateLongTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseBooleanTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseDoubleTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseFloatTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseLongTransform;
import dev.sbs.dataflow.stage.transform.primitive.PeekTransform;
import dev.sbs.dataflow.stage.transform.primitive.ToStringTransform;
import dev.sbs.dataflow.stage.transform.string.LowerCaseTransform;
import dev.sbs.dataflow.stage.transform.string.PrefixTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.string.ReplaceTransform;
import dev.sbs.dataflow.stage.transform.string.SplitTransform;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import dev.sbs.dataflow.stage.transform.string.SuffixTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import dev.sbs.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Stable wire-format discriminator for a {@link Stage} kind.
 * <p>
 * Each constant binds a wire name to the implementation class. Display name, description,
 * category, configuration schema, and the deserialisation factory are all derived
 * reflectively on first touch by {@link StageReflection}, then cached. The constant itself
 * is just an identifier paired with the class.
 * <p>
 * Adding a stage means appending one entry here plus authoring the class with
 * {@link StageSpec} and {@link Configurable} annotations on its canonical factory.
 */
public enum StageKind {

    /* ---- Source ---- */
    SOURCE_EMBED(EmbedSource.class),
    SOURCE_LITERAL(LiteralSource.class),
    SOURCE_LITERAL_LIST(LiteralListSource.class),
    SOURCE_URL(UrlSource.class),

    /* ---- Filter ---- */
    FILTER_DISTINCT(DistinctFilter.class),
    FILTER_DOM_HAS_ATTR(DomHasAttrFilter.class),
    FILTER_DOM_TAG_EQUALS(DomTagEqualsFilter.class),
    FILTER_DOM_TEXT_CONTAINS(DomTextContainsFilter.class),
    FILTER_DOM_TEXT_MATCHES(DomTextMatchesFilter.class),
    FILTER_DOUBLE_GREATER_THAN(DoubleGreaterThanFilter.class),
    FILTER_DOUBLE_IN_RANGE(DoubleInRangeFilter.class),
    FILTER_DOUBLE_LESS_THAN(DoubleLessThanFilter.class),
    FILTER_DROP_WHILE(DropWhileFilter.class),
    FILTER_INDEX_IN_RANGE(IndexInRangeFilter.class),
    FILTER_INT_GREATER_THAN(IntGreaterThanFilter.class),
    FILTER_INT_IN_RANGE(IntInRangeFilter.class),
    FILTER_INT_LESS_THAN(IntLessThanFilter.class),
    FILTER_JSON_FIELD_EQUALS(JsonFieldEqualsFilter.class),
    FILTER_JSON_HAS_FIELD(JsonHasFieldFilter.class),
    FILTER_LONG_GREATER_THAN(LongGreaterThanFilter.class),
    FILTER_LONG_IN_RANGE(LongInRangeFilter.class),
    FILTER_LONG_LESS_THAN(LongLessThanFilter.class),
    FILTER_NOT_NULL(NotNullFilter.class),
    FILTER_SKIP(SkipFilter.class),
    FILTER_STRING_CONTAINS(StringContainsFilter.class),
    FILTER_STRING_ENDS_WITH(StringEndsWithFilter.class),
    FILTER_STRING_EQUALS(StringEqualsFilter.class),
    FILTER_STRING_MATCHES(StringMatchesFilter.class),
    FILTER_STRING_NON_EMPTY(StringNonEmptyFilter.class),
    FILTER_STRING_STARTS_WITH(StringStartsWithFilter.class),
    FILTER_TAKE(TakeFilter.class),
    FILTER_TAKE_WHILE(TakeWhileFilter.class),

    /* ---- Transform ---- */
    PARSE_HTML(ParseHtmlTransform.class),
    PARSE_JSON(ParseJsonTransform.class),
    PARSE_XML(ParseXmlTransform.class),
    TRANSFORM_ABS_DOUBLE(AbsDoubleTransform.class),
    TRANSFORM_ABS_FLOAT(AbsFloatTransform.class),
    TRANSFORM_ABS_INT(AbsIntTransform.class),
    TRANSFORM_ABS_LONG(AbsLongTransform.class),
    TRANSFORM_BASE64_DECODE(Base64DecodeTransform.class),
    TRANSFORM_BASE64_ENCODE(Base64EncodeTransform.class),
    TRANSFORM_CSS_SELECT(CssSelectTransform.class),
    TRANSFORM_DOM_CHILDREN(DomChildrenTransform.class),
    TRANSFORM_DOM_OUTER_HTML(DomOuterHtmlTransform.class),
    TRANSFORM_DOM_OWN_TEXT(DomOwnTextTransform.class),
    TRANSFORM_DOM_PARENT(DomParentTransform.class),
    TRANSFORM_FLATTEN(FlattenTransform.class),
    TRANSFORM_FLAT_MAP(FlatMapTransform.class),
    TRANSFORM_GSON_DESERIALIZE(GsonDeserializeTransform.class),
    TRANSFORM_JSON_AS_BOOLEAN(JsonAsBooleanTransform.class),
    TRANSFORM_JSON_AS_DOUBLE(JsonAsDoubleTransform.class),
    TRANSFORM_JSON_AS_INT(JsonAsIntTransform.class),
    TRANSFORM_JSON_AS_LONG(JsonAsLongTransform.class),
    TRANSFORM_JSON_AS_STRING(JsonAsStringTransform.class),
    TRANSFORM_JSON_FIELD(JsonFieldTransform.class),
    TRANSFORM_JSON_OBJECT_BUILD(JsonObjectBuildTransform.class),
    TRANSFORM_JSON_PATH(JsonPathTransform.class),
    TRANSFORM_JSON_STRINGIFY(JsonStringifyTransform.class),
    TRANSFORM_LIST_LENGTH(ListLengthTransform.class),
    TRANSFORM_LOWERCASE(LowerCaseTransform.class),
    TRANSFORM_MAP(MapTransform.class),
    TRANSFORM_NEGATE_DOUBLE(NegateDoubleTransform.class),
    TRANSFORM_NEGATE_FLOAT(NegateFloatTransform.class),
    TRANSFORM_NEGATE_INT(NegateIntTransform.class),
    TRANSFORM_NEGATE_LONG(NegateLongTransform.class),
    TRANSFORM_NODE_ATTR(NodeAttrTransform.class),
    TRANSFORM_NODE_TEXT(NodeTextTransform.class),
    TRANSFORM_NTH_CHILD(NthChildTransform.class),
    TRANSFORM_PARSE_BOOLEAN(ParseBooleanTransform.class),
    TRANSFORM_PARSE_DOUBLE(ParseDoubleTransform.class),
    TRANSFORM_PARSE_FLOAT(ParseFloatTransform.class),
    TRANSFORM_PARSE_INT(ParseIntTransform.class),
    TRANSFORM_PARSE_LONG(ParseLongTransform.class),
    TRANSFORM_PEEK(PeekTransform.class),
    TRANSFORM_PREFIX(PrefixTransform.class),
    TRANSFORM_REGEX_EXTRACT(RegexExtractTransform.class),
    TRANSFORM_REPLACE(ReplaceTransform.class),
    TRANSFORM_REVERSE(ReverseTransform.class),
    TRANSFORM_SORT(SortTransform.class),
    TRANSFORM_SORT_BY(SortByTransform.class),
    TRANSFORM_SPLIT(SplitTransform.class),
    TRANSFORM_STRING_LENGTH(StringLengthTransform.class),
    TRANSFORM_SUFFIX(SuffixTransform.class),
    TRANSFORM_TO_STRING(ToStringTransform.class),
    TRANSFORM_TRIM(TrimTransform.class),
    TRANSFORM_UPPERCASE(UpperCaseTransform.class),
    TRANSFORM_URL_DECODE(UrlDecodeTransform.class),
    TRANSFORM_URL_ENCODE(UrlEncodeTransform.class),

    /* ---- Predicate ---- */
    PREDICATE_AND(AndPredicate.class),
    PREDICATE_DOM_HAS_ATTR(DomHasAttrPredicate.class),
    PREDICATE_DOM_TAG_EQUALS(DomTagEqualsPredicate.class),
    PREDICATE_DOM_TEXT_CONTAINS(DomTextContainsPredicate.class),
    PREDICATE_DOM_TEXT_MATCHES(DomTextMatchesPredicate.class),
    PREDICATE_DOUBLE_GREATER_THAN(DoubleGreaterThanPredicate.class),
    PREDICATE_DOUBLE_IN_RANGE(DoubleInRangePredicate.class),
    PREDICATE_DOUBLE_LESS_THAN(DoubleLessThanPredicate.class),
    PREDICATE_INT_GREATER_THAN(IntGreaterThanPredicate.class),
    PREDICATE_INT_IN_RANGE(IntInRangePredicate.class),
    PREDICATE_INT_LESS_THAN(IntLessThanPredicate.class),
    PREDICATE_JSON_FIELD_EQUALS(JsonFieldEqualsPredicate.class),
    PREDICATE_JSON_HAS_FIELD(JsonHasFieldPredicate.class),
    PREDICATE_LONG_GREATER_THAN(LongGreaterThanPredicate.class),
    PREDICATE_LONG_IN_RANGE(LongInRangePredicate.class),
    PREDICATE_LONG_LESS_THAN(LongLessThanPredicate.class),
    PREDICATE_NOT(NotPredicate.class),
    PREDICATE_NOT_NULL(NotNullPredicate.class),
    PREDICATE_OR(OrPredicate.class),
    PREDICATE_STRING_CONTAINS(StringContainsPredicate.class),
    PREDICATE_STRING_ENDS_WITH(StringEndsWithPredicate.class),
    PREDICATE_STRING_EQUALS(StringEqualsPredicate.class),
    PREDICATE_STRING_MATCHES(StringMatchesPredicate.class),
    PREDICATE_STRING_NON_EMPTY(StringNonEmptyPredicate.class),
    PREDICATE_STRING_STARTS_WITH(StringStartsWithPredicate.class),

    /* ---- Collect ---- */
    COLLECT_ALL_MATCH(AllMatchCollect.class),
    COLLECT_ANY_MATCH(AnyMatchCollect.class),
    COLLECT_AVERAGE_DOUBLE(AverageDoubleCollect.class),
    COLLECT_AVERAGE_INT(AverageIntCollect.class),
    COLLECT_AVERAGE_LONG(AverageLongCollect.class),
    COLLECT_COUNT(CountCollect.class),
    COLLECT_FIND_FIRST(FindFirstCollect.class),
    COLLECT_FIRST(FirstCollect.class),
    COLLECT_JOIN(JoinCollect.class),
    COLLECT_JSON_OBJECT_FROM_ENTRIES(JsonObjectFromEntriesCollect.class),
    COLLECT_LAST(LastCollect.class),
    COLLECT_LIST(ListCollect.class),
    COLLECT_MAP(MapCollect.class),
    COLLECT_MAX(MaxCollect.class),
    COLLECT_MAX_BY(MaxByCollect.class),
    COLLECT_MIN(MinCollect.class),
    COLLECT_MIN_BY(MinByCollect.class),
    COLLECT_NONE_MATCH(NoneMatchCollect.class),
    COLLECT_SET(SetCollect.class),
    COLLECT_SUM_DOUBLE(SumDoubleCollect.class),
    COLLECT_SUM_INT(SumIntCollect.class),
    COLLECT_SUM_LONG(SumLongCollect.class);

    /**
     * Cached snapshot of {@link #values()} reused by lookups to avoid the per-call defensive array clone.
     */
    private static final StageKind @NotNull [] CACHED_VALUES = values();

    private final @NotNull Class<? extends Stage<?, ?>> stageClass;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    StageKind(@NotNull Class<? extends Stage> stageClass) {
        this.stageClass = (Class<? extends Stage<?, ?>>) stageClass;
    }

    /**
     * Returns the human-friendly display name from this kind's {@link StageSpec}.
     *
     * @return the display name
     */
    public @NotNull String displayName() {
        return metadata().annotation().displayName();
    }

    /**
     * Returns the short type-signature description from this kind's {@link StageSpec}.
     *
     * @return the description
     */
    public @NotNull String description() {
        return metadata().annotation().description();
    }

    /**
     * Returns the coarse {@link StageSpec.Category} grouping for this kind.
     *
     * @return the category
     */
    public @NotNull StageSpec.Category category() {
        return metadata().annotation().category();
    }

    /**
     * Returns the configuration schema derived from the canonical factory's
     * {@link Configurable} parameters.
     *
     * @return the schema, in factory parameter order
     */
    public @NotNull List<FieldSpec<?>> schema() {
        return metadata().schema();
    }

    /**
     * Returns the deserialisation factory that rebuilds a stage from a populated
     * {@link StageConfig}. The lambda delegates to the cached {@link StageMetadata}.
     *
     * @return the factory
     */
    public @NotNull Function<StageConfig, Stage<?, ?>> factory() {
        return cfg -> metadata().fromConfig(cfg);
    }

    /**
     * Returns the implementation class this kind binds to.
     *
     * @return the stage class
     */
    public @NotNull Class<? extends Stage<?, ?>> stageClass() {
        return this.stageClass;
    }

    private @NotNull StageMetadata metadata() {
        return StageReflection.of(this.stageClass);
    }

    /**
     * Returns every {@link StageKind} in {@code category}, in declaration order.
     *
     * @param category the category
     * @return the kinds in {@code category}, possibly empty
     */
    public static @NotNull List<StageKind> ofCategory(@NotNull StageSpec.Category category) {
        return Arrays.stream(CACHED_VALUES)
            .filter(id -> id.category() == category)
            .toList();
    }

}

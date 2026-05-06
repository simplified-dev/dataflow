package dev.sbs.dataflow.serde;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.branch.Branch;
import dev.sbs.dataflow.stage.collect.FirstCollect;
import dev.sbs.dataflow.stage.collect.JoinCollect;
import dev.sbs.dataflow.stage.collect.LastCollect;
import dev.sbs.dataflow.stage.collect.ListCollect;
import dev.sbs.dataflow.stage.collect.SetCollect;
import dev.sbs.dataflow.stage.embed.PipelineEmbed;
import dev.sbs.dataflow.stage.filter.DistinctFilter;
import dev.sbs.dataflow.stage.filter.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.ParseJsonTransform;
import dev.sbs.dataflow.stage.transform.ParseXmlTransform;
import dev.sbs.dataflow.stage.transform.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.JsonFieldTransform;
import dev.sbs.dataflow.stage.transform.JsonPathTransform;
import dev.sbs.dataflow.stage.transform.NodeAttrTransform;
import dev.sbs.dataflow.stage.transform.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.NthChildTransform;
import dev.sbs.dataflow.stage.transform.ParseDoubleTransform;
import dev.sbs.dataflow.stage.transform.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.ReplaceTransform;
import dev.sbs.dataflow.stage.transform.SplitTransform;
import dev.sbs.dataflow.stage.transform.TrimTransform;
import dev.simplified.gson.factory.CaseInsensitiveEnumTypeAdapterFactory;
import dev.simplified.gson.factory.PostInitTypeAdapterFactory;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;

/**
 * Gson-based serialiser for {@link DataPipeline} definitions.
 * <p>
 * Wire format is a JSON array of stage descriptors. Each descriptor carries a {@code "kind"}
 * field whose value is the {@link StageId} name; the remaining fields are the stage's
 * configuration. {@link DataType} references serialise as their {@link DataType#label()},
 * round-tripped through {@link DataTypes#byLabel(String)}.
 * <p>
 * The internal {@link Gson} instance is configured with the {@code gson-extras}
 * {@link CaseInsensitiveEnumTypeAdapterFactory} and {@link PostInitTypeAdapterFactory} so
 * upstream consumers stay consistent with the rest of the platform.
 */
@UtilityClass
public final class PipelineGson {

    private static final @NotNull Gson GSON = new GsonBuilder()
        .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
        .registerTypeAdapterFactory(new PostInitTypeAdapterFactory())
        .disableHtmlEscaping()
        .create();

    /**
     * Serialises a {@link DataPipeline} into its on-disk JSON form.
     *
     * @param pipeline the pipeline to serialise
     * @return the JSON definition
     */
    public static @NotNull String toJson(@NotNull DataPipeline pipeline) {
        return GSON.toJson(toJsonArray(pipeline));
    }

    /**
     * Deserialises a {@link DataPipeline} from its on-disk JSON form.
     *
     * @param json the JSON definition
     * @return the rebuilt pipeline
     * @throws IllegalArgumentException if the JSON references an unknown {@link StageId} or
     *         a {@link DataType} label that this build does not recognise
     */
    public static @NotNull DataPipeline fromJson(@NotNull String json) {
        JsonElement el = JsonParser.parseString(json);
        if (!el.isJsonArray())
            throw new IllegalArgumentException("Pipeline JSON must be a top-level array");
        return fromJsonArray(el.getAsJsonArray());
    }

    /* ====================  internals  ==================== */

    @SuppressWarnings("unchecked")
    private static @NotNull JsonArray toJsonArray(@NotNull DataPipeline pipeline) {
        JsonArray arr = new JsonArray();
        for (Stage<?, ?> stage : pipeline.stages())
            arr.add(stageToJson(stage));
        return arr;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static @NotNull DataPipeline fromJsonArray(@NotNull JsonArray arr) {
        if (arr.isEmpty()) return DataPipeline.empty();
        DataPipeline.Builder b = DataPipeline.builder();
        boolean first = true;
        for (JsonElement el : arr) {
            Stage<?, ?> stage = stageFromJson(el.getAsJsonObject());
            if (first) {
                b.source((Stage<Void, ?>) stage);
                first = false;
            } else {
                b.stage(stage);
            }
        }
        return b.build();
    }

    private static @NotNull JsonObject stageToJson(@NotNull Stage<?, ?> stage) {
        JsonObject o = new JsonObject();
        o.addProperty("kind", stage.kind().name());
        switch (stage.kind()) {
            case SOURCE_URL -> {
                UrlSource s = (UrlSource) stage;
                o.addProperty("url", s.url());
                o.addProperty("outputType", s.outputType().label());
            }
            case SOURCE_PASTE -> {
                PasteSource s = (PasteSource) stage;
                o.addProperty("body", s.body());
                o.addProperty("outputType", s.outputType().label());
            }
            case PARSE_HTML, PARSE_XML, PARSE_JSON,
                 TRANSFORM_NODE_TEXT,
                 TRANSFORM_PARSE_INT, TRANSFORM_PARSE_DOUBLE,
                 TRANSFORM_TRIM -> {
                /* config-free */
            }
            case TRANSFORM_CSS_SELECT -> o.addProperty("selector", ((CssSelectTransform) stage).selector());
            case TRANSFORM_NODE_ATTR -> o.addProperty("attributeName", ((NodeAttrTransform) stage).attributeName());
            case TRANSFORM_NTH_CHILD -> {
                NthChildTransform s = (NthChildTransform) stage;
                o.addProperty("childSelector", s.childSelector());
                o.addProperty("index", s.index());
            }
            case TRANSFORM_JSON_PATH -> o.addProperty("path", ((JsonPathTransform) stage).path());
            case TRANSFORM_JSON_FIELD -> o.addProperty("fieldName", ((JsonFieldTransform) stage).fieldName());
            case TRANSFORM_REGEX_EXTRACT -> {
                RegexExtractTransform s = (RegexExtractTransform) stage;
                o.addProperty("regex", s.regex());
                o.addProperty("group", s.group());
            }
            case TRANSFORM_REPLACE -> {
                ReplaceTransform s = (ReplaceTransform) stage;
                o.addProperty("regex", s.regex());
                o.addProperty("replacement", s.replacement());
            }
            case TRANSFORM_SPLIT -> o.addProperty("regex", ((SplitTransform) stage).regex());
            case TRANSFORM_MAP -> throw new UnsupportedOperationException("TRANSFORM_MAP serde lands in v2");
            case FILTER_DOM_TEXT_CONTAINS -> o.addProperty("needle", ((DomTextContainsFilter) stage).needle());
            case FILTER_DISTINCT -> o.addProperty("elementType", ((DistinctFilter<?>) stage).elementType().label());
            case COLLECT_FIRST -> o.addProperty("elementType", ((FirstCollect<?>) stage).elementType().label());
            case COLLECT_LAST -> o.addProperty("elementType", ((LastCollect<?>) stage).elementType().label());
            case COLLECT_LIST -> o.addProperty("elementType", ((ListCollect<?>) stage).elementType().label());
            case COLLECT_SET -> o.addProperty("elementType", ((SetCollect<?>) stage).elementType().label());
            case COLLECT_JOIN -> o.addProperty("separator", ((JoinCollect) stage).separator());
            case BRANCH -> {
                Branch<?> b = (Branch<?>) stage;
                o.addProperty("inputType", b.inputType().label());
                JsonObject outputs = new JsonObject();
                for (var entry : b.outputs().entrySet()) {
                    JsonArray sub = new JsonArray();
                    for (Stage<?, ?> child : entry.getValue())
                        sub.add(stageToJson(child));
                    outputs.add(entry.getKey(), sub);
                }
                o.add("outputs", outputs);
            }
            case PIPELINE_EMBED -> {
                PipelineEmbed<?> e = (PipelineEmbed<?>) stage;
                o.addProperty("embeddedPipelineId", e.embeddedPipelineId());
                o.addProperty("outputType", e.outputType().label());
            }
        }
        return o;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static @NotNull Stage<?, ?> stageFromJson(@NotNull JsonObject o) {
        StageId kind = StageId.valueOf(o.get("kind").getAsString());
        return switch (kind) {
            case SOURCE_URL -> {
                String url = o.get("url").getAsString();
                String label = o.get("outputType").getAsString();
                yield switch (label) {
                    case "RAW_HTML" -> UrlSource.html(url);
                    case "RAW_XML" -> UrlSource.xml(url);
                    case "RAW_JSON" -> UrlSource.json(url);
                    case "RAW_TEXT" -> UrlSource.text(url);
                    default -> throw new IllegalArgumentException("Unknown UrlSource outputType: " + label);
                };
            }
            case SOURCE_PASTE -> {
                String body = o.get("body").getAsString();
                String label = o.get("outputType").getAsString();
                yield switch (label) {
                    case "RAW_HTML" -> PasteSource.html(body);
                    case "RAW_XML" -> PasteSource.xml(body);
                    case "RAW_JSON" -> PasteSource.json(body);
                    case "RAW_TEXT" -> PasteSource.text(body);
                    default -> throw new IllegalArgumentException("Unknown PasteSource outputType: " + label);
                };
            }
            case PARSE_HTML -> ParseHtmlTransform.create();
            case PARSE_XML -> ParseXmlTransform.create();
            case PARSE_JSON -> ParseJsonTransform.create();
            case TRANSFORM_CSS_SELECT -> CssSelectTransform.of(o.get("selector").getAsString());
            case TRANSFORM_NODE_TEXT -> NodeTextTransform.create();
            case TRANSFORM_NODE_ATTR -> NodeAttrTransform.of(o.get("attributeName").getAsString());
            case TRANSFORM_NTH_CHILD -> NthChildTransform.of(
                o.get("childSelector").getAsString(),
                o.get("index").getAsInt()
            );
            case TRANSFORM_JSON_PATH -> JsonPathTransform.of(o.get("path").getAsString());
            case TRANSFORM_JSON_FIELD -> JsonFieldTransform.of(o.get("fieldName").getAsString());
            case TRANSFORM_REGEX_EXTRACT -> RegexExtractTransform.of(
                o.get("regex").getAsString(),
                o.has("group") ? o.get("group").getAsInt() : 0
            );
            case TRANSFORM_PARSE_INT -> ParseIntTransform.create();
            case TRANSFORM_PARSE_DOUBLE -> ParseDoubleTransform.create();
            case TRANSFORM_TRIM -> TrimTransform.create();
            case TRANSFORM_REPLACE -> ReplaceTransform.of(
                o.get("regex").getAsString(),
                o.get("replacement").getAsString()
            );
            case TRANSFORM_SPLIT -> SplitTransform.of(o.get("regex").getAsString());
            case TRANSFORM_MAP -> throw new UnsupportedOperationException("TRANSFORM_MAP serde lands in v2");
            case FILTER_DOM_TEXT_CONTAINS -> DomTextContainsFilter.of(o.get("needle").getAsString());
            case FILTER_DISTINCT -> DistinctFilter.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_FIRST -> FirstCollect.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_LAST -> LastCollect.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_LIST -> ListCollect.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_SET -> SetCollect.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_JOIN -> JoinCollect.of(o.get("separator").getAsString());
            case BRANCH -> {
                DataType<?> input = requireType(o.get("inputType").getAsString());
                Branch.Builder<?> bb = Branch.over((DataType) input);
                JsonObject outputs = o.getAsJsonObject("outputs");
                for (var entry : outputs.entrySet()) {
                    String name = entry.getKey();
                    JsonArray sub = entry.getValue().getAsJsonArray();
                    bb.output(name, chain -> {
                        for (JsonElement child : sub)
                            chain.stage(stageFromJson(child.getAsJsonObject()));
                    });
                }
                yield bb.build();
            }
            case PIPELINE_EMBED -> PipelineEmbed.of(
                o.get("embeddedPipelineId").getAsString(),
                requireType(o.get("outputType").getAsString())
            );
        };
    }

    private static @NotNull DataType<?> requireType(@NotNull String label) {
        DataType<?> t = DataTypes.byLabel(label);
        if (t == null)
            throw new IllegalArgumentException("Unknown DataType label: '" + label + "'");
        return t;
    }

}

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
import dev.sbs.dataflow.stage.collect.CollectFirst;
import dev.sbs.dataflow.stage.collect.CollectJoin;
import dev.sbs.dataflow.stage.collect.CollectLast;
import dev.sbs.dataflow.stage.collect.CollectList;
import dev.sbs.dataflow.stage.collect.CollectSet;
import dev.sbs.dataflow.stage.embed.PipelineEmbed;
import dev.sbs.dataflow.stage.filter.FilterDistinct;
import dev.sbs.dataflow.stage.filter.FilterDomTextContains;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.ParseJsonTransform;
import dev.sbs.dataflow.stage.transform.ParseXmlTransform;
import dev.sbs.dataflow.stage.transform.TransformCssSelect;
import dev.sbs.dataflow.stage.transform.TransformJsonField;
import dev.sbs.dataflow.stage.transform.TransformJsonPath;
import dev.sbs.dataflow.stage.transform.TransformNodeAttr;
import dev.sbs.dataflow.stage.transform.TransformNodeText;
import dev.sbs.dataflow.stage.transform.TransformNthChild;
import dev.sbs.dataflow.stage.transform.TransformParseDouble;
import dev.sbs.dataflow.stage.transform.TransformParseInt;
import dev.sbs.dataflow.stage.transform.TransformRegexExtract;
import dev.sbs.dataflow.stage.transform.TransformReplace;
import dev.sbs.dataflow.stage.transform.TransformSplit;
import dev.sbs.dataflow.stage.transform.TransformTrim;
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
            case TRANSFORM_CSS_SELECT -> o.addProperty("selector", ((TransformCssSelect) stage).selector());
            case TRANSFORM_NODE_ATTR -> o.addProperty("attributeName", ((TransformNodeAttr) stage).attributeName());
            case TRANSFORM_NTH_CHILD -> {
                TransformNthChild s = (TransformNthChild) stage;
                o.addProperty("childSelector", s.childSelector());
                o.addProperty("index", s.index());
            }
            case TRANSFORM_JSON_PATH -> o.addProperty("path", ((TransformJsonPath) stage).path());
            case TRANSFORM_JSON_FIELD -> o.addProperty("fieldName", ((TransformJsonField) stage).fieldName());
            case TRANSFORM_REGEX_EXTRACT -> {
                TransformRegexExtract s = (TransformRegexExtract) stage;
                o.addProperty("regex", s.regex());
                o.addProperty("group", s.group());
            }
            case TRANSFORM_REPLACE -> {
                TransformReplace s = (TransformReplace) stage;
                o.addProperty("regex", s.regex());
                o.addProperty("replacement", s.replacement());
            }
            case TRANSFORM_SPLIT -> o.addProperty("regex", ((TransformSplit) stage).regex());
            case TRANSFORM_MAP -> throw new UnsupportedOperationException("TRANSFORM_MAP serde lands in v2");
            case FILTER_DOM_TEXT_CONTAINS -> o.addProperty("needle", ((FilterDomTextContains) stage).needle());
            case FILTER_DISTINCT -> o.addProperty("elementType", ((FilterDistinct<?>) stage).elementType().label());
            case COLLECT_FIRST -> o.addProperty("elementType", ((CollectFirst<?>) stage).elementType().label());
            case COLLECT_LAST -> o.addProperty("elementType", ((CollectLast<?>) stage).elementType().label());
            case COLLECT_LIST -> o.addProperty("elementType", ((CollectList<?>) stage).elementType().label());
            case COLLECT_SET -> o.addProperty("elementType", ((CollectSet<?>) stage).elementType().label());
            case COLLECT_JOIN -> o.addProperty("separator", ((CollectJoin) stage).separator());
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
            case TRANSFORM_CSS_SELECT -> TransformCssSelect.of(o.get("selector").getAsString());
            case TRANSFORM_NODE_TEXT -> TransformNodeText.create();
            case TRANSFORM_NODE_ATTR -> TransformNodeAttr.of(o.get("attributeName").getAsString());
            case TRANSFORM_NTH_CHILD -> TransformNthChild.of(
                o.get("childSelector").getAsString(),
                o.get("index").getAsInt()
            );
            case TRANSFORM_JSON_PATH -> TransformJsonPath.of(o.get("path").getAsString());
            case TRANSFORM_JSON_FIELD -> TransformJsonField.of(o.get("fieldName").getAsString());
            case TRANSFORM_REGEX_EXTRACT -> TransformRegexExtract.of(
                o.get("regex").getAsString(),
                o.has("group") ? o.get("group").getAsInt() : 0
            );
            case TRANSFORM_PARSE_INT -> TransformParseInt.create();
            case TRANSFORM_PARSE_DOUBLE -> TransformParseDouble.create();
            case TRANSFORM_TRIM -> TransformTrim.create();
            case TRANSFORM_REPLACE -> TransformReplace.of(
                o.get("regex").getAsString(),
                o.get("replacement").getAsString()
            );
            case TRANSFORM_SPLIT -> TransformSplit.of(o.get("regex").getAsString());
            case TRANSFORM_MAP -> throw new UnsupportedOperationException("TRANSFORM_MAP serde lands in v2");
            case FILTER_DOM_TEXT_CONTAINS -> FilterDomTextContains.of(o.get("needle").getAsString());
            case FILTER_DISTINCT -> FilterDistinct.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_FIRST -> CollectFirst.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_LAST -> CollectLast.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_LIST -> CollectList.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_SET -> CollectSet.of(requireType(o.get("elementType").getAsString()));
            case COLLECT_JOIN -> CollectJoin.of(o.get("separator").getAsString());
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

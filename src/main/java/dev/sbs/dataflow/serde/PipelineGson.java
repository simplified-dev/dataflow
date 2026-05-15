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
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.ChainSerde;
import dev.sbs.dataflow.chain.NamedChains;
import dev.sbs.dataflow.chain.TypedChain;
import dev.sbs.dataflow.stage.FieldSpec;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.simplified.gson.factory.CaseInsensitiveEnumTypeAdapterFactory;
import dev.simplified.gson.factory.PostInitTypeAdapterFactory;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Gson-based serialiser for {@link DataPipeline} definitions.
 * <p>
 * Wire format is a JSON array of stage descriptors. Each descriptor carries a {@code "kind"}
 * field whose value is the {@link StageKind} name; the remaining fields are the stage's
 * configuration. {@link DataType} references serialise as their {@link DataType#label()},
 * round-tripped through {@link DataTypes#byLabel(String)}.
 * <p>
 * The schema and factory used by serde live directly on each {@link StageKind} constant.
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
     * Returns the shared {@link Gson} instance used for pipeline serde. Reuse it from
     * stages that need Gson-backed coercion (e.g. {@code JsonObjectBuildTransform},
     * {@code GsonDeserializeTransform}) so the configuration stays consistent.
     *
     * @return the shared Gson instance
     */
    public static @NotNull Gson gson() {
        return GSON;
    }

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
     * @throws IllegalArgumentException if the JSON references an unknown {@link StageKind} or
     *         a {@link DataType} label that this build does not recognise
     */
    public static @NotNull DataPipeline fromJson(@NotNull String json) {
        JsonElement el = JsonParser.parseString(json);

        if (!el.isJsonArray())
            throw new IllegalArgumentException("Pipeline JSON must be a top-level array");

        return fromJsonArray(el.getAsJsonArray());
    }

    /* ====================  internals  ==================== */

    private static @NotNull JsonArray toJsonArray(@NotNull DataPipeline pipeline) {
        JsonArray arr = new JsonArray();

        for (Stage<?, ?> stage : pipeline.stages())
            arr.add(stageToJson(stage));

        return arr;
    }

    @SuppressWarnings("unchecked")
    private static @NotNull DataPipeline fromJsonArray(@NotNull JsonArray arr) {
        if (arr.isEmpty()) return DataPipeline.empty();
        DataPipeline.Builder b = DataPipeline.builder();
        boolean first = true;

        for (JsonElement el : arr) {
            Stage<?, ?> stage = stageFromJson(el.getAsJsonObject());
            if (first) {
                b.source((SourceStage<?>) stage);
                first = false;
            } else
                b.stage(stage);
        }

        return b.build();
    }

    @SuppressWarnings("unchecked")
    private static @NotNull JsonObject stageToJson(@NotNull Stage<?, ?> stage) {
        JsonObject o = new JsonObject();
        StageKind kind = stage.kind();
        o.addProperty("kind", kind.name());
        StageConfig cfg = stage.config();

        for (FieldSpec spec : kind.schema()) {
            Object v = cfg.raw(spec.name());
            switch (spec.type()) {
                case STRING    -> o.addProperty(spec.name(), (String) v);
                case INT       -> o.addProperty(spec.name(), (Integer) v);
                case LONG      -> o.addProperty(spec.name(), (Long) v);
                case DOUBLE    -> o.addProperty(spec.name(), (Double) v);
                case BOOLEAN   -> o.addProperty(spec.name(), (Boolean) v);
                case DATA_TYPE -> o.addProperty(spec.name(), ((DataType<?>) v).label());
                case SUB_PIPELINE          -> o.add(spec.name(), ChainSerde.writeChain((Chain) v, PipelineGson::stageToJson));
                case SUB_PIPELINES_MAP     -> o.add(spec.name(), ChainSerde.writeNamedChains((NamedChains) v, PipelineGson::stageToJson));
                case TYPED_SUB_PIPELINES_MAP -> o.add(spec.name(), ChainSerde.writeTypedNamedChains((Map<String, TypedChain>) v, PipelineGson::stageToJson));
            }
        }
        return o;
    }

    private static @NotNull Stage<?, ?> stageFromJson(@NotNull JsonObject o) {
        StageKind kind = StageKind.valueOf(o.get("kind").getAsString());
        if (kind.factory() == null)
            throw new IllegalArgumentException("No factory registered for kind: " + kind);
        StageConfig.Builder b = StageConfig.builder();

        for (FieldSpec spec : kind.schema()) {
            JsonElement raw = o.get(spec.name());
            if (raw == null) continue;

            switch (spec.type()) {
                case STRING    -> b.string(spec.name(), raw.getAsString());
                case INT       -> b.integer(spec.name(), raw.getAsInt());
                case LONG      -> b.longVal(spec.name(), raw.getAsLong());
                case DOUBLE    -> b.doubleVal(spec.name(), raw.getAsDouble());
                case BOOLEAN   -> b.bool(spec.name(), raw.getAsBoolean());
                case DATA_TYPE -> b.dataType(spec.name(), requireType(raw.getAsString()));
                case SUB_PIPELINE          -> b.subPipeline(spec.name(), ChainSerde.readChain(raw.getAsJsonArray(), PipelineGson::stageFromJson));
                case SUB_PIPELINES_MAP     -> b.subPipelines(spec.name(), ChainSerde.readNamedChains(raw.getAsJsonObject(), PipelineGson::stageFromJson));
                case TYPED_SUB_PIPELINES_MAP -> b.typedSubPipelines(spec.name(), ChainSerde.readTypedNamedChains(raw.getAsJsonObject(), PipelineGson::stageFromJson));
            }
        }
        return kind.factory().apply(b.build());
    }

    private static @NotNull DataType<?> requireType(@NotNull String label) {
        DataType<?> t = DataTypes.byLabel(label);

        if (t == null)
            throw new IllegalArgumentException("Unknown DataType label: '" + label + "'");

        return t;
    }

}

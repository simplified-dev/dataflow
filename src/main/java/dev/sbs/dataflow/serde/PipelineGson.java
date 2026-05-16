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
import dev.sbs.dataflow.stage.FieldSpec;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageMetadata;
import dev.sbs.dataflow.stage.StageReflection;
import dev.sbs.dataflow.stage.StageRegistry;
import dev.sbs.dataflow.stage.StageSpec;
import dev.simplified.gson.factory.CaseInsensitiveEnumTypeAdapterFactory;
import dev.simplified.gson.factory.PostInitTypeAdapterFactory;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;

/**
 * Gson-based serialiser for {@link DataPipeline} definitions.
 * <p>
 * Wire format is a JSON array of stage descriptors. Each descriptor carries a {@code "kind"}
 * field whose value is the {@link StageSpec#id()} of the stage's class; the remaining fields
 * are the stage's configuration. {@link DataType} references serialise as their
 * {@link DataType#label()}, round-tripped through {@link DataTypes#byLabel(String)}.
 * <p>
 * Per-slot JSON read/write dispatch lives on {@link FieldSpec#writeJson} / {@link FieldSpec#readJson};
 * this class just iterates the {@link StageMetadata#schema()} of the resolved class and threads
 * the recursive stage callbacks for nested sub-pipelines.
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
     * @throws IllegalArgumentException if the JSON references an unknown stage id or
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
        o.addProperty("kind", stage.kindId());
        StageMetadata metadata = StageReflection.of((Class<? extends Stage<?, ?>>) stage.getClass());
        StageConfig cfg = stage.config();

        for (FieldSpec<?> spec : metadata.schema()) {
            Object v = cfg.raw(spec.name());
            if (v != null) o.add(spec.name(), spec.writeJson(v, PipelineGson::stageToJson));
        }
        return o;
    }

    private static @NotNull Stage<?, ?> stageFromJson(@NotNull JsonObject o) {
        String id = o.get("kind").getAsString();
        Class<? extends Stage<?, ?>> cls = StageRegistry.byId(id);
        StageMetadata metadata = StageReflection.of(cls);
        StageConfig.Builder b = StageConfig.builder();

        for (FieldSpec<?> spec : metadata.schema()) {
            JsonElement raw = o.get(spec.name());
            if (raw != null) spec.readJson(raw, b, PipelineGson::stageFromJson);
        }
        return metadata.fromConfig(b.build());
    }

}

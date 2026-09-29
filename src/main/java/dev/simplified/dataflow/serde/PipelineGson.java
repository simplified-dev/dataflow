package dev.simplified.dataflow.serde;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import dev.simplified.annotations.UtilityClass;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.chain.ChainSerde;
import dev.simplified.dataflow.stage.FieldSpec;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.StageConfig;
import dev.simplified.dataflow.stage.StageRegistry;
import dev.simplified.dataflow.stage.meta.StageMetadata;
import dev.simplified.dataflow.stage.meta.StageReflection;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.gson.factory.CaseInsensitiveEnumTypeAdapterFactory;
import dev.simplified.gson.factory.PostInitTypeAdapterFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

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
 * the recursive stage callbacks for nested sub-pipelines. A pipeline operand is a nested stage
 * array read and validated by {@link ChainSerde#readPipeline} exactly as the top-level file is.
 * <p>
 * The internal {@link Gson} instance is configured with the {@code gson-extras}
 * {@link CaseInsensitiveEnumTypeAdapterFactory} and {@link PostInitTypeAdapterFactory} so
 * upstream consumers stay consistent with the rest of the platform.
 */
@UtilityClass
public final class PipelineGson {

    /**
     * Key of a stage descriptor holding the {@link StageSpec#id()} of the stage's class.
     */
    private static final @NotNull String KIND = "kind";

    private static final @NotNull Gson GSON = new GsonBuilder()
        .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
        .registerTypeAdapterFactory(new PostInitTypeAdapterFactory())
        .disableHtmlEscaping()
        .create();

    /**
     * Returns the shared {@link Gson} instance used for pipeline serde. Reuse it from
     * stages that need Gson-backed coercion (e.g. {@code ObjectBuildTransform},
     * {@code DeserializeTransform}) so the configuration stays consistent.
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
    public static @NotNull String toJson(@NotNull DataPipeline<?> pipeline) {
        return GSON.toJson(toJsonArray(pipeline));
    }

    /**
     * Deserialises a {@link DataPipeline} from its on-disk JSON form. The returned pipeline
     * has a wildcard output type; callers wanting a typed handle should narrow via
     * {@link DataPipeline#expectOutput(DataType)}.
     * <p>
     * Every stage, at any depth, is read strictly: each key besides {@code "kind"} must be one of
     * the stage's slots, every required slot must be present, and a JSON {@code null} reads as the
     * key being absent. No object may name a key twice, since only one of the two values would be
     * read. A stage factory that refuses its values fails the load with the exception the factory
     * threw.
     *
     * @param json the JSON definition
     * @return the rebuilt pipeline
     * @throws IllegalArgumentException if the JSON references an unknown stage id or
     *         a {@link DataType} label that this build does not recognise, an object names a key
     *         twice, a stage lacks its {@code "kind"} or a required key, holds a key it does not
     *         declare or a value of the wrong JSON shape, or a stage factory refuses its values
     * @throws IllegalStateException if the stages, or the stages of a pipeline operand, do not
     *         form a valid pipeline
     */
    public static @NotNull DataPipeline<?> fromJson(@NotNull String json) {
        JsonElement el = JsonParser.parseString(json);

        if (!el.isJsonArray())
            throw new IllegalArgumentException("Pipeline JSON must be a top-level array");

        requireUniqueKeys(json);
        return fromJsonArray(el.getAsJsonArray());
    }

    /* ====================  internals  ==================== */

    /**
     * Checks that no object in {@code json} names a key twice. A parsed tree cannot show it,
     * because the later value replaces the earlier one there, so the text is walked token by token
     * with the leniency {@link JsonParser} reads it with.
     *
     * @param json JSON text that {@link JsonParser} has already parsed
     * @throws IllegalArgumentException when an object names a key twice
     */
    private static void requireUniqueKeys(@NotNull String json) {
        JsonReader reader = new JsonReader(new StringReader(json));
        reader.setStrictness(Strictness.LENIENT);
        Deque<Set<String>> objects = new ArrayDeque<>();

        try {
            while (true) {
                switch (reader.peek()) {
                    case BEGIN_OBJECT -> {
                        reader.beginObject();
                        objects.push(new HashSet<>());
                    }
                    case END_OBJECT -> {
                        reader.endObject();
                        objects.pop();
                    }
                    case BEGIN_ARRAY -> reader.beginArray();
                    case END_ARRAY -> reader.endArray();
                    case NAME -> {
                        String name = reader.nextName();

                        if (!objects.getFirst().add(name)) {
                            throw new IllegalArgumentException(String.format(
                                "Pipeline JSON repeats key '%s' at '%s'", name, reader.getPath()
                            ));
                        }
                    }
                    case END_DOCUMENT -> {
                        return;
                    }
                    default -> reader.skipValue();
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static @NotNull JsonArray toJsonArray(@NotNull DataPipeline<?> pipeline) {
        return ChainSerde.writePipeline(pipeline, PipelineGson::stageToJson);
    }

    private static @NotNull DataPipeline<?> fromJsonArray(@NotNull JsonArray arr) {
        return ChainSerde.readPipeline(arr, PipelineGson::stageFromJson);
    }

    @SuppressWarnings("unchecked")
    private static @NotNull JsonObject stageToJson(@NotNull Stage<?, ?> stage) {
        JsonObject o = new JsonObject();
        o.addProperty(KIND, stage.kindId());
        StageMetadata metadata = StageReflection.of((Class<? extends Stage<?, ?>>) stage.getClass());
        StageConfig cfg = stage.config();

        for (FieldSpec<?> spec : metadata.schema()) {
            Object v = cfg.raw(spec.name());
            if (v != null) o.add(spec.name(), spec.writeJson(v, PipelineGson::stageToJson));
        }
        return o;
    }

    private static @NotNull Stage<?, ?> stageFromJson(@NotNull JsonObject o) {
        JsonElement kind = o.get(KIND);

        if (kind == null || kind.isJsonNull())
            throw new IllegalArgumentException(String.format("Stage entry is missing required key '%s'", KIND));

        if (!kind.isJsonPrimitive()) {
            throw new IllegalArgumentException(String.format(
                "Stage entry holds %s under '%s' but a stage id was expected", ChainSerde.shapeOf(kind), KIND
            ));
        }

        Class<? extends Stage<?, ?>> cls = StageRegistry.byId(kind.getAsString());
        StageMetadata metadata = StageReflection.of(cls);
        metadata.requireDeclared(o.keySet().stream().filter(key -> !KIND.equals(key)).toList());
        StageConfig.Builder b = StageConfig.builder();

        for (FieldSpec<?> spec : metadata.schema()) {
            JsonElement raw = o.get(spec.name());
            if (raw != null) spec.readJson(raw, b, PipelineGson::stageFromJson);
        }

        return metadata.fromConfig(b.build());
    }

}

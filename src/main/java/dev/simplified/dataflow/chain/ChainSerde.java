package dev.simplified.dataflow.chain;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.stage.Stage;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Wire-format helpers for the three chain shapes and the sourced pipeline carried by
 * {@code StageConfig}. The host serialiser supplies callbacks that handle per-stage JSON
 * conversion; this class owns only the shape iteration, and the validation of a sourced
 * pipeline read back from its stage array.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ChainSerde {

    /**
     * Key of a typed sub-pipeline entry holding the label of the type its chain produces.
     */
    private static final @NotNull String OUTPUT_TYPE = "outputType";

    /**
     * Key of a typed sub-pipeline entry holding its stage array.
     */
    private static final @NotNull String CHAIN = "chain";

    /**
     * Every key a typed sub-pipeline entry may hold, in the order a load error lists them.
     */
    private static final @NotNull List<String> TYPED_KEYS = List.of(OUTPUT_TYPE, CHAIN);

    /**
     * Serialises a {@link Chain} as a JSON array of stage objects.
     *
     * @param chain the chain to serialise
     * @param stageWriter callback that converts a single stage to its JSON form
     * @return the resulting JSON array
     */
    public static @NotNull JsonArray writeChain(
        @NotNull Chain<?, ?> chain,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonArray arr = new JsonArray();
        for (Stage<?, ?> stage : chain.stages())
            arr.add(stageWriter.apply(stage));
        return arr;
    }

    /**
     * Deserialises a JSON array into a {@link Chain}. The returned chain has wildcard
     * input/output types because the wire format does not carry them statically.
     *
     * @param arr the JSON array
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt chain
     * @throws IllegalArgumentException when an entry of the array is not a JSON object
     */
    public static @NotNull Chain<?, ?> readChain(
        @NotNull JsonArray arr,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        return Chain.unchecked(readStages(arr, stageReader));
    }

    /**
     * Serialises a sourced {@link DataPipeline} as a JSON array of stage objects, the shape of a
     * pipeline file.
     *
     * @param pipeline the pipeline to serialise
     * @param stageWriter callback that converts a single stage to its JSON form
     * @return the resulting JSON array
     */
    public static @NotNull JsonArray writePipeline(
        @NotNull DataPipeline<?> pipeline,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonArray arr = new JsonArray();
        for (Stage<?, ?> stage : pipeline.stages())
            arr.add(stageWriter.apply(stage));
        return arr;
    }

    /**
     * Deserialises a JSON array in the shape of a pipeline file into a validated
     * {@link DataPipeline}. An empty array reads as {@link DataPipeline#empty()}.
     * <p>
     * The wire format carries no static type, so the last stage's runtime
     * {@link Stage#outputType()} becomes the pipeline's output type, and
     * {@link DataPipeline#validate()} checks the type chain - stage 0 a source, every later
     * stage consuming the output of the one before it.
     *
     * @param arr the JSON array
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt pipeline
     * @throws IllegalArgumentException when an entry of the array is not a JSON object
     * @throws IllegalStateException when the stages do not form a valid pipeline
     */
    public static @NotNull DataPipeline<?> readPipeline(
        @NotNull JsonArray arr,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        if (arr.isEmpty()) return DataPipeline.empty();
        List<Stage<?, ?>> stages = readStages(arr, stageReader);
        DataPipeline<?> pipeline = DataPipeline.unchecked(stages, stages.getLast().outputType());
        ValidationReport report = pipeline.validate();

        if (!report.isValid())
            throw new IllegalStateException("Cannot build invalid pipeline: " + report.issues());

        return pipeline;
    }

    /**
     * Serialises a {@link NamedChains} as a JSON object whose values are stage arrays.
     *
     * @param chains the named chains
     * @param stageWriter callback that converts a single stage to its JSON form
     * @return the resulting JSON object
     */
    public static @NotNull JsonObject writeNamedChains(
        @NotNull NamedChains<?> chains,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonObject out = new JsonObject();
        for (Map.Entry<String, ? extends Chain<?, ?>> entry : chains.chains().entrySet())
            out.add(entry.getKey(), writeChain(entry.getValue(), stageWriter));
        return out;
    }

    /**
     * Deserialises a JSON object of named stage arrays into a {@link NamedChains}, keeping the
     * document order of the names.
     *
     * @param obj the JSON object
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt named chains
     * @throws IllegalArgumentException when a name maps to anything but a stage array, or an entry of
     *         one is not a JSON object
     */
    public static @NotNull NamedChains<?> readNamedChains(
        @NotNull JsonObject obj,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        LinkedHashMap<String, Chain<Object, ?>> map = new LinkedHashMap<>();

        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            if (!entry.getValue().isJsonArray()) {
                throw new IllegalArgumentException(String.format(
                    "Sub-pipeline '%s' must be a stage array but was %s", entry.getKey(), shapeOf(entry.getValue())
                ));
            }

            map.put(entry.getKey(), Chain.of(readStages(entry.getValue().getAsJsonArray(), stageReader)));
        }

        return new NamedChains<>(map);
    }

    /**
     * Serialises a typed named-chains map as a JSON object whose values carry their declared
     * output type plus the stage array.
     *
     * @param chains the typed named chains
     * @param stageWriter callback that converts a single stage to its JSON form
     * @return the resulting JSON object
     */
    public static @NotNull JsonObject writeTypedNamedChains(
        @NotNull Map<String, TypedChain<?>> chains,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonObject out = new JsonObject();
        for (Map.Entry<String, TypedChain<?>> entry : chains.entrySet()) {
            JsonObject typed = new JsonObject();
            typed.addProperty(OUTPUT_TYPE, entry.getValue().outputType().label());
            typed.add(CHAIN, writeChain(entry.getValue().chain(), stageWriter));
            out.add(entry.getKey(), typed);
        }
        return out;
    }

    /**
     * Deserialises a typed named-chains JSON object into an unmodifiable
     * {@code Map<String, TypedChain<?>>} that keeps the document order of the names.
     * <p>
     * Each entry is an object holding exactly {@code outputType} and {@code chain}; a JSON
     * {@code null} on either reads as the key being absent.
     *
     * @param obj the JSON object
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt typed named-chains map
     * @throws IllegalArgumentException if any entry is not an object, lacks either key, holds a key
     *         besides them, holds either in the wrong shape, or references an unknown
     *         {@link DataType} label
     */
    public static @NotNull Map<String, TypedChain<?>> readTypedNamedChains(
        @NotNull JsonObject obj,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        LinkedHashMap<String, TypedChain<?>> map = new LinkedHashMap<>();

        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            String name = entry.getKey();

            if (!entry.getValue().isJsonObject()) {
                throw new IllegalArgumentException(String.format(
                    "Typed sub-pipeline '%s' must be a JSON object but was %s", name, shapeOf(entry.getValue())
                ));
            }

            JsonObject typed = entry.getValue().getAsJsonObject();

            for (String key : typed.keySet()) {
                if (!TYPED_KEYS.contains(key)) {
                    throw new IllegalArgumentException(String.format(
                        "Typed sub-pipeline '%s' does not declare key '%s' (declared keys: %s)", name, key, TYPED_KEYS
                    ));
                }
            }

            JsonElement rawLabel = typedEntry(name, typed, OUTPUT_TYPE);
            JsonElement rawChain = typedEntry(name, typed, CHAIN);

            if (!rawLabel.isJsonPrimitive()) {
                throw new IllegalArgumentException(String.format(
                    "Typed sub-pipeline '%s' holds %s under '%s' but a type label was expected", name, shapeOf(rawLabel), OUTPUT_TYPE
                ));
            }

            if (!rawChain.isJsonArray()) {
                throw new IllegalArgumentException(String.format(
                    "Typed sub-pipeline '%s' holds %s under '%s' but a stage array was expected", name, shapeOf(rawChain), CHAIN
                ));
            }

            String label = rawLabel.getAsString();
            DataType<?> outputType = DataTypes.byLabel(label);

            if (outputType == null) {
                throw new IllegalArgumentException(String.format(
                    "Typed sub-pipeline '%s' holds unknown DataType label '%s' under '%s'", name, label, OUTPUT_TYPE
                ));
            }

            List<Stage<?, ?>> stages = readStages(rawChain.getAsJsonArray(), stageReader);
            map.put(name, typedChainOf(outputType, stages));
        }

        return Concurrent.newUnmodifiableLinkedMap(map);
    }

    /**
     * Names the JSON shape of an element for a load error - {@code a JSON object},
     * {@code a JSON array}, {@code a JSON primitive} or {@code a JSON null}.
     *
     * @param element the element to describe
     * @return the shape, with its article
     */
    public static @NotNull String shapeOf(@NotNull JsonElement element) {
        if (element.isJsonObject()) return "a JSON object";
        if (element.isJsonArray()) return "a JSON array";
        if (element.isJsonPrimitive()) return "a JSON primitive";
        return "a JSON null";
    }

    private static @NotNull JsonElement typedEntry(@NotNull String name, @NotNull JsonObject typed, @NotNull String key) {
        JsonElement value = typed.get(key);

        if (value == null || value.isJsonNull()) {
            throw new IllegalArgumentException(String.format(
                "Typed sub-pipeline '%s' is missing required key '%s'", name, key
            ));
        }

        return value;
    }

    private static @NotNull List<Stage<?, ?>> readStages(
        @NotNull JsonArray arr,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        List<Stage<?, ?>> stages = new ArrayList<>(arr.size());

        for (JsonElement el : arr) {
            if (!el.isJsonObject()) {
                throw new IllegalArgumentException(String.format(
                    "Stage entry must be a JSON object but was %s", shapeOf(el)
                ));
            }

            stages.add(stageReader.apply(el.getAsJsonObject()));
        }

        return stages;
    }

    private static <O> @NotNull TypedChain<O> typedChainOf(
        @NotNull DataType<O> outputType,
        @NotNull List<Stage<?, ?>> stages
    ) {
        return new TypedChain<>(outputType, Chain.of(stages));
    }

}

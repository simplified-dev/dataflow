package dev.simplified.dataflow.chain;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
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
     */
    public static @NotNull Chain<?, ?> readChain(
        @NotNull JsonArray arr,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        List<Stage<?, ?>> stages = new ArrayList<>(arr.size());
        for (JsonElement el : arr)
            stages.add(stageReader.apply(el.getAsJsonObject()));
        return Chain.unchecked(stages);
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
     * Deserialises a JSON object of named stage arrays into a {@link NamedChains}.
     *
     * @param obj the JSON object
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt named chains
     */
    public static @NotNull NamedChains<?> readNamedChains(
        @NotNull JsonObject obj,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        LinkedHashMap<String, Chain<Object, ?>> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet())
            map.put(entry.getKey(), Chain.of(readStages(entry.getValue().getAsJsonArray(), stageReader)));
        return new NamedChains<>(Map.copyOf(map));
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
            typed.addProperty("outputType", entry.getValue().outputType().label());
            typed.add("chain", writeChain(entry.getValue().chain(), stageWriter));
            out.add(entry.getKey(), typed);
        }
        return out;
    }

    /**
     * Deserialises a typed named-chains JSON object into a {@code Map<String, TypedChain<?>>}.
     *
     * @param obj the JSON object
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt typed named-chains map
     * @throws IllegalArgumentException if any entry references an unknown {@link DataType} label
     */
    public static @NotNull Map<String, TypedChain<?>> readTypedNamedChains(
        @NotNull JsonObject obj,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        LinkedHashMap<String, TypedChain<?>> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            JsonObject typed = entry.getValue().getAsJsonObject();
            String label = typed.get("outputType").getAsString();
            DataType<?> outputType = DataTypes.byLabel(label);
            if (outputType == null)
                throw new IllegalArgumentException("Unknown DataType label: '" + label + "'");
            List<Stage<?, ?>> stages = readStages(typed.get("chain").getAsJsonArray(), stageReader);
            map.put(entry.getKey(), typedChainOf(outputType, stages));
        }
        return Map.copyOf(map);
    }

    private static @NotNull List<Stage<?, ?>> readStages(
        @NotNull JsonArray arr,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        List<Stage<?, ?>> stages = new ArrayList<>(arr.size());
        for (JsonElement el : arr)
            stages.add(stageReader.apply(el.getAsJsonObject()));
        return stages;
    }

    private static <O> @NotNull TypedChain<O> typedChainOf(
        @NotNull DataType<O> outputType,
        @NotNull List<Stage<?, ?>> stages
    ) {
        return new TypedChain<>(outputType, Chain.of(stages));
    }

}

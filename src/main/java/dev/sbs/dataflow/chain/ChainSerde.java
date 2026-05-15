package dev.sbs.dataflow.chain;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.stage.Stage;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Wire-format helpers for the three chain shapes carried by {@code StageConfig}. The host
 * serialiser supplies callbacks that handle per-stage JSON conversion; this class owns only
 * the chain-shape iteration.
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
        @NotNull Chain chain,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonArray arr = new JsonArray();
        for (Stage<?, ?> stage : chain.stages())
            arr.add(stageWriter.apply(stage));
        return arr;
    }

    /**
     * Deserialises a JSON array into a {@link Chain}.
     *
     * @param arr the JSON array
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt chain
     */
    public static @NotNull Chain readChain(
        @NotNull JsonArray arr,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        List<Stage<?, ?>> stages = new ArrayList<>(arr.size());
        for (JsonElement el : arr)
            stages.add(stageReader.apply(el.getAsJsonObject()));
        return Chain.of(stages);
    }

    /**
     * Serialises a {@link NamedChains} as a JSON object whose values are stage arrays.
     *
     * @param chains the named chains
     * @param stageWriter callback that converts a single stage to its JSON form
     * @return the resulting JSON object
     */
    public static @NotNull JsonObject writeNamedChains(
        @NotNull NamedChains chains,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonObject out = new JsonObject();
        for (Map.Entry<String, Chain> entry : chains.chains().entrySet())
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
    public static @NotNull NamedChains readNamedChains(
        @NotNull JsonObject obj,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        LinkedHashMap<String, Chain> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet())
            map.put(entry.getKey(), readChain(entry.getValue().getAsJsonArray(), stageReader));
        return new NamedChains(Map.copyOf(map));
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
        @NotNull Map<String, TypedChain> chains,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        JsonObject out = new JsonObject();
        for (Map.Entry<String, TypedChain> entry : chains.entrySet()) {
            JsonObject typed = new JsonObject();
            typed.addProperty("outputType", entry.getValue().outputType().label());
            typed.add("chain", writeChain(entry.getValue().chain(), stageWriter));
            out.add(entry.getKey(), typed);
        }
        return out;
    }

    /**
     * Deserialises a typed named-chains JSON object into a {@code Map<String, TypedChain>}.
     *
     * @param obj the JSON object
     * @param stageReader callback that rebuilds a single stage from its JSON form
     * @return the rebuilt typed named-chains map
     * @throws IllegalArgumentException if any entry references an unknown {@link DataType} label
     */
    public static @NotNull Map<String, TypedChain> readTypedNamedChains(
        @NotNull JsonObject obj,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        LinkedHashMap<String, TypedChain> map = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            JsonObject typed = entry.getValue().getAsJsonObject();
            String label = typed.get("outputType").getAsString();
            DataType<?> outputType = DataTypes.byLabel(label);
            if (outputType == null)
                throw new IllegalArgumentException("Unknown DataType label: '" + label + "'");
            Chain chain = readChain(typed.get("chain").getAsJsonArray(), stageReader);
            map.put(entry.getKey(), new TypedChain(outputType, chain));
        }
        return Map.copyOf(map);
    }

}

package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.terminal.collect.MapCollect;
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * {@link TransformStage} that reads two lists out of one value and pairs them by position.
 * <p>
 * Both bodies run against the same input, the way {@link MapCollect} branches do, so the stage
 * reads two sibling lists out of one element. The k-th output holds the k-th element of the left
 * list under {@code leftKey} and the k-th element of the right list under {@code rightKey}.
 * {@link Mode#SHORTEST} stops at the end of the shorter list; {@link Mode#LONGEST} runs to the end
 * of the longer one and omits the key of the side that has run out. A {@code null} element omits
 * its key the same way, as an {@link ObjectBuildTransform} output omits a {@code null}, and so does
 * an element JSON cannot hold - a {@code NaN} or infinite {@code FLOAT} or {@code DOUBLE}, or a list
 * holding one. A position where both sides are absent is an empty object, so every output stays at
 * its position.
 * An element is written as an {@link ObjectBuildTransform} output is, and a {@link JsonElement}
 * element is copied.
 *
 * @param <I> input type, shared by both bodies
 * @param <L> left element type
 * @param <R> right element type
 */
@StageSpec(
    id = "TRANSFORM_ZIP",
    displayName = "Zip by position",
    description = "I -> List<JSON_OBJECT> (left: I -> List<L>, right: I -> List<R>)",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ZipTransform<I, L, R> implements TransformStage<I, List<JsonObject>> {

    private static final @NotNull DataType<List<JsonObject>> LIST_OBJ = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<L> leftType;

    private final @NotNull DataType<R> rightType;

    private final @NotNull Chain<I, List<L>> left;

    private final @NotNull Chain<I, List<R>> right;

    private final @NotNull String leftKey;

    private final @NotNull String rightKey;

    /**
     * Configured mode exactly as given, carried on the wire under {@code mode}, or {@code null}
     * when the slot is absent.
     */
    private final @Nullable String rawMode;

    /**
     * Where the pairing stops, {@link Mode#SHORTEST} unless configured.
     */
    private final @NotNull Mode mode;

    /**
     * Stopping point of a {@link ZipTransform} whose lists differ in length.
     */
    public enum Mode {

        /**
         * Stops at the end of the shorter list.
         */
        SHORTEST,

        /**
         * Runs to the end of the longer list, omitting the key of the side that has run out.
         */
        LONGEST

    }

    /**
     * Constructs a zip stage.
     *
     * @param inputType the input type both bodies consume
     * @param leftType element type of the left list; a type Gson can write
     * @param rightType element type of the right list; a type Gson can write
     * @param left sub-pipeline that maps {@code I} to {@code List<L>}
     * @param right sub-pipeline that maps {@code I} to {@code List<R>}
     * @param leftKey key each output object holds the left element under
     * @param rightKey key each output object holds the right element under
     * @param rawMode a {@link Mode} name, or {@code null} for {@link Mode#SHORTEST}; carried on
     *                the wire as {@code mode}
     * @return the stage
     * @param <I> input type
     * @param <L> left element type
     * @param <R> right element type
     * @throws IllegalArgumentException when either body fails type-chain validation, an element
     *         type cannot be written as JSON, the two keys are the same, or {@code rawMode} is not
     *         a {@link Mode} name
     */
    public static <I, L, R> @NotNull ZipTransform<I, L, R> of(
        @Configurable(label = "Input type", placeholder = "JSON_OBJECT")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Left element type", placeholder = "STRING")
        @NotNull DataType<L> leftType,
        @Configurable(label = "Right element type", placeholder = "STRING")
        @NotNull DataType<R> rightType,
        @Configurable(label = "Left list body (yields List<L>)")
        @NotNull List<? extends Stage<?, ?>> left,
        @Configurable(label = "Right list body (yields List<R>)")
        @NotNull List<? extends Stage<?, ?>> right,
        @Configurable(label = "Left key", placeholder = "id")
        @NotNull String leftKey,
        @Configurable(label = "Right key", placeholder = "rarity")
        @NotNull String rightKey,
        @Configurable(name = "mode", label = "Mode (optional)", placeholder = "SHORTEST", optional = true)
        @Nullable String rawMode
    ) {
        ValidationReport leftReport = Chain.validate(inputType, left, DataType.list(leftType));

        if (!leftReport.isValid())
            throw new IllegalArgumentException("Invalid ZipTransform left body: " + leftReport.issues());

        ValidationReport rightReport = Chain.validate(inputType, right, DataType.list(rightType));

        if (!rightReport.isValid())
            throw new IllegalArgumentException("Invalid ZipTransform right body: " + rightReport.issues());

        JsonValues.requireWritable(leftType, "ZipTransform", "leftType");
        JsonValues.requireWritable(rightType, "ZipTransform", "rightType");

        if (leftKey.equals(rightKey)) {
            throw new IllegalArgumentException(String.format(
                "Invalid ZipTransform keys: both sides are keyed '%s'", leftKey
            ));
        }

        return new ZipTransform<>(
            inputType,
            leftType,
            rightType,
            Chain.of(left),
            Chain.of(right),
            leftKey,
            rightKey,
            rawMode,
            parseMode(rawMode)
        );
    }

    private static @NotNull Mode parseMode(@Nullable String rawMode) {
        if (rawMode == null) return Mode.SHORTEST;

        for (Mode mode : Mode.values())
            if (mode.name().equals(rawMode)) return mode;

        throw new IllegalArgumentException(String.format(
            "Invalid ZipTransform mode '%s', expected one of %s", rawMode, Arrays.toString(Mode.values())
        ));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;
        List<L> lefts = this.left.execute(ctx, input);
        if (lefts == null) return null;
        List<R> rights = this.right.execute(ctx, input);
        if (rights == null) return null;

        int size = this.mode == Mode.LONGEST
            ? Math.max(lefts.size(), rights.size())
            : Math.min(lefts.size(), rights.size());
        Iterator<L> leftIterator = lefts.iterator();
        Iterator<R> rightIterator = rights.iterator();
        List<JsonObject> result = new ArrayList<>(size);

        for (int position = 0; position < size; position++) {
            JsonObject pair = new JsonObject();
            put(pair, this.leftKey, leftIterator.hasNext() ? leftIterator.next() : null);
            put(pair, this.rightKey, rightIterator.hasNext() ? rightIterator.next() : null);
            result.add(pair);
        }

        return Concurrent.newUnmodifiableList(result);
    }

    private static void put(@NotNull JsonObject pair, @NotNull String key, @Nullable Object element) {
        JsonElement value = JsonValues.toJson(element);
        if (value != null) pair.add(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return LIST_OBJ;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Zip " + this.inputType.label() + " -> {" + this.leftKey + ": " + this.leftType.label()
            + ", " + this.rightKey + ": " + this.rightType.label() + "} " + this.mode;
    }

}

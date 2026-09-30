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
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link TransformStage} that reads one parent value and a list of children out of one value and
 * carries the parent onto every child.
 * <p>
 * Both bodies run against the same input. There is one output per child, in order, each
 * {@code {parentKey: parent, childKey: child}}. A {@code null} parent omits {@code parentKey} from
 * every output, as an {@link ObjectBuildTransform} output omits a {@code null}, and a {@code null}
 * child is dropped. A parent or child JSON cannot hold - a {@code NaN} or infinite {@code FLOAT} or
 * {@code DOUBLE}, or a list holding one - counts as {@code null}. It is a {@link ZipTransform} with
 * one side held constant. A value is written
 * as an {@link ObjectBuildTransform} output is, and a {@link JsonElement} value is copied into
 * each output, so no two outputs share a tree.
 *
 * @param <I> input type, shared by both bodies
 * @param <P> parent value type
 * @param <C> child element type
 */
@StageSpec(
    id = "TRANSFORM_BROADCAST",
    displayName = "Broadcast parent onto children",
    description = "I -> List<JSON_OBJECT> (parent: I -> P, children: I -> List<C>)",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class BroadcastTransform<I, P, C> implements TransformStage<I, List<JsonObject>> {

    private static final @NotNull DataType<List<JsonObject>> LIST_OBJ = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<P> parentType;

    private final @NotNull DataType<C> childType;

    private final @NotNull Chain<I, P> parent;

    private final @NotNull Chain<I, List<C>> children;

    private final @NotNull String parentKey;

    private final @NotNull String childKey;

    /**
     * Constructs a broadcast stage.
     *
     * @param inputType the input type both bodies consume
     * @param parentType the parent value type; a type Gson can write
     * @param childType element type of the children list; a type Gson can write
     * @param parent sub-pipeline that maps {@code I} to {@code P}
     * @param children sub-pipeline that maps {@code I} to {@code List<C>}
     * @param parentKey key each output object holds the parent under
     * @param childKey key each output object holds its child under
     * @return the stage
     * @param <I> input type
     * @param <P> parent value type
     * @param <C> child element type
     * @throws IllegalArgumentException when either body fails type-chain validation, a value type
     *         cannot be written as JSON, or the two keys are the same
     */
    public static <I, P, C> @NotNull BroadcastTransform<I, P, C> of(
        @Configurable(label = "Input type", placeholder = "DOM_NODE")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Parent type", placeholder = "STRING")
        @NotNull DataType<P> parentType,
        @Configurable(label = "Child type", placeholder = "STRING")
        @NotNull DataType<C> childType,
        @Configurable(label = "Parent body (yields P)")
        @NotNull List<? extends Stage<?, ?>> parent,
        @Configurable(label = "Children body (yields List<C>)")
        @NotNull List<? extends Stage<?, ?>> children,
        @Configurable(label = "Parent key", placeholder = "symbol")
        @NotNull String parentKey,
        @Configurable(label = "Child key", placeholder = "usage")
        @NotNull String childKey
    ) {
        ValidationReport parentReport = Chain.validate(inputType, parent, parentType);

        if (!parentReport.isValid())
            throw new IllegalArgumentException("Invalid BroadcastTransform parent body: " + parentReport.issues());

        ValidationReport childrenReport = Chain.validate(inputType, children, DataType.list(childType));

        if (!childrenReport.isValid())
            throw new IllegalArgumentException("Invalid BroadcastTransform children body: " + childrenReport.issues());

        JsonValues.requireWritable(parentType, "BroadcastTransform", "parentType");
        JsonValues.requireWritable(childType, "BroadcastTransform", "childType");

        if (parentKey.equals(childKey)) {
            throw new IllegalArgumentException(String.format(
                "Invalid BroadcastTransform keys: the parent and the children are both keyed '%s'", parentKey
            ));
        }

        return new BroadcastTransform<>(
            inputType,
            parentType,
            childType,
            Chain.of(parent),
            Chain.of(children),
            parentKey,
            childKey
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;
        P value = this.parent.execute(ctx, input);
        List<C> elements = this.children.execute(ctx, input);
        if (elements == null) return null;
        JsonElement shared = JsonValues.toJson(value);
        List<JsonObject> result = new ArrayList<>(elements.size());

        for (C child : elements) {
            JsonElement written = JsonValues.toJson(child);
            if (written == null) continue;
            JsonObject row = new JsonObject();
            if (shared != null) row.add(this.parentKey, shared.deepCopy());
            row.add(this.childKey, written);
            result.add(row);
        }

        return Concurrent.newUnmodifiableList(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return LIST_OBJ;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Broadcast " + this.parentKey + ": " + this.parentType.label()
            + " onto each " + this.childKey + ": " + this.childType.label();
    }

}

package dev.simplified.dataflow.stage;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.chain.NamedChains;
import dev.simplified.dataflow.stage.meta.StageMetadata;
import dev.simplified.dataflow.stage.meta.StageReflection;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Checks, for every {@link StageRegistry registered stage}, that the instance field named after
 * each configurable factory parameter can hold that slot's configuration value.
 * <p>
 * {@link StageMetadata#buildConfig(Stage)} reads that field to answer {@link Stage#config()}, so
 * a stage that parses a string parameter into another type and stores the parsed value under
 * the parameter's name hands the wrong type to the slot, and {@code config()} and
 * {@code PipelineGson.toJson} throw {@link ClassCastException}.
 */
class StageFieldConventionTest {

    private static final @NotNull Map<FieldSpec.Type, Class<?>> STORAGE = Map.ofEntries(
        Map.entry(FieldSpec.Type.STRING, String.class),
        Map.entry(FieldSpec.Type.INT, Integer.class),
        Map.entry(FieldSpec.Type.LONG, Long.class),
        Map.entry(FieldSpec.Type.DOUBLE, Double.class),
        Map.entry(FieldSpec.Type.BOOLEAN, Boolean.class),
        Map.entry(FieldSpec.Type.DATA_TYPE, DataType.class),
        Map.entry(FieldSpec.Type.SUB_PIPELINE, Chain.class),
        Map.entry(FieldSpec.Type.SUB_PIPELINES_MAP, NamedChains.class),
        Map.entry(FieldSpec.Type.TYPED_SUB_PIPELINES_MAP, Map.class),
        Map.entry(FieldSpec.Type.PIPELINE, DataPipeline.class),
        Map.entry(FieldSpec.Type.STRING_MAP, Map.class)
    );

    private static @NotNull Class<?> boxed(@NotNull Class<?> type) {
        if (type == int.class) return Integer.class;
        if (type == long.class) return Long.class;
        if (type == double.class) return Double.class;
        if (type == boolean.class) return Boolean.class;
        return type;
    }

    private static @NotNull Field field(@NotNull Class<?> cls, @NotNull String name) throws NoSuchFieldException {
        for (Class<?> c = cls; c != null; c = c.getSuperclass()) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) { }
        }
        throw new NoSuchFieldException(cls.getName() + "." + name);
    }

    @TestFactory
    Stream<DynamicTest> everySlotFieldHoldsItsConfigValue() {
        return StageRegistry.allOrdered().stream().flatMap(cls -> {
            StageMetadata metadata = StageReflection.of(cls);
            String id = metadata.annotation().id();
            return metadata.slots().stream().map(slot -> DynamicTest.dynamicTest(id + "." + slot.paramName(), () -> {
                Class<?> declared = boxed(field(cls, slot.paramName()).getType());
                Class<?> storage = STORAGE.get(slot.spec().type());
                assertThat(
                    "field '" + slot.paramName() + "' is " + declared.getSimpleName() + " but its "
                        + slot.spec().type() + " slot stores " + storage.getSimpleName(),
                    storage.isAssignableFrom(declared), is(true)
                );
            }));
        });
    }

}

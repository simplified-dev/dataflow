package dev.sbs.dataflow.stage.meta;

import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.FieldSpec;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.NamedChains;
import dev.sbs.dataflow.chain.TypedChain;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.reflection.accessor.MethodAccessor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * First-touch reflection cache for {@link Stage} implementation classes.
 * <p>
 * Reads the class-level {@link StageSpec} annotation, locates the canonical
 * {@code public static of(...)} factory method, and derives the ordered list of
 * configurable {@link StageMetadata.Slot slots} from the factory's {@link Configurable} parameter
 * annotations. Each slot tracks both the wire-side {@link FieldSpec} and the matching
 * Java parameter / instance-field name (since a few stages override the wire key via
 * {@code @Configurable(name = ...)}).
 * <p>
 * Sub-pipeline factory parameter conventions are bridged via per-slot
 * {@link StageMetadata.Slot#argAdapter adapters}. The wire format always stores
 * {@link Chain} / {@link NamedChains}, but factories may accept the looser
 * {@code List<? extends Stage<?, ?>>} / {@code Map<String, List<? extends Stage<?, ?>>>}
 * shapes for ergonomics. The derivation table:
 * <ul>
 *   <li>{@link Chain} -> {@link FieldSpec.Type#SUB_PIPELINE}, identity adapter</li>
 *   <li>{@code List<? extends Stage<?, ?>>} -> {@link FieldSpec.Type#SUB_PIPELINE}, adapter {@code chain -> chain.stages()}</li>
 *   <li>{@link NamedChains} -> {@link FieldSpec.Type#SUB_PIPELINES_MAP}, identity adapter</li>
 *   <li>{@code Map<String, ? extends List<? extends Stage<?, ?>>>} -> {@link FieldSpec.Type#SUB_PIPELINES_MAP}, adapter {@code named -> {name -> chain.stages()}}</li>
 *   <li>{@code Map<String, TypedChain>} -> {@link FieldSpec.Type#TYPED_SUB_PIPELINES_MAP}, identity adapter</li>
 * </ul>
 * The derived {@link StageMetadata} is cached per class so each subsequent lookup is a map
 * read.
 * <p>
 * Built on {@link Reflection} from {@code dev.simplified.reflection}, which provides
 * per-class JNI-once caches for declared fields / methods and handles
 * {@code setAccessible(true)} internally.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StageReflection {

    private static final @NotNull Pattern GENERATED_ARG_NAME = Pattern.compile("arg\\d+");

    private static final @NotNull ConcurrentMap<Class<? extends Stage<?, ?>>, StageMetadata> CACHE = Concurrent.newMap();

    private static final @NotNull Function<Object, Object> CHAIN_TO_STAGES =
        raw -> ((Chain) raw).stages();

    private static final @NotNull Function<Object, Object> NAMED_CHAINS_TO_LISTS = raw -> {
        NamedChains named = (NamedChains) raw;
        Map<String, List<Stage<?, ?>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Chain> entry : named.chains().entrySet())
            result.put(entry.getKey(), entry.getValue().stages());
        return result;
    };

    /**
     * Returns the cached {@link StageMetadata} for the given stage class, deriving it on
     * first touch.
     *
     * @param stageClass the stage implementation class
     * @return the cached metadata
     */
    public static @NotNull StageMetadata of(@NotNull Class<? extends Stage<?, ?>> stageClass) {
        return CACHE.computeIfAbsent(stageClass, StageReflection::derive);
    }

    private static @NotNull StageMetadata derive(@NotNull Class<? extends Stage<?, ?>> stageClass) {
        StageSpec annotation = stageClass.getAnnotation(StageSpec.class);
        if (annotation == null)
            throw new IllegalStateException("Missing @StageSpec on " + stageClass.getName());

        Reflection<?> reflection = new Reflection<>(stageClass);
        Method factoryMethod = findCanonicalFactory(stageClass);
        MethodAccessor<?> factoryAccessor = new MethodAccessor<>(reflection, factoryMethod);

        List<StageMetadata.Slot<?>> slots = Arrays.stream(factoryMethod.getParameters())
            .filter(p -> p.isAnnotationPresent(Configurable.class))
            .map(p -> slotOf(p, reflection))
            .collect(Collectors.toUnmodifiableList());

        return new StageMetadata(annotation, slots, factoryAccessor);
    }

    private static @NotNull Method findCanonicalFactory(@NotNull Class<?> stageClass) {
        List<Method> candidates = Arrays.stream(stageClass.getDeclaredMethods())
            .filter(m -> Modifier.isStatic(m.getModifiers()))
            .filter(m -> "of".equals(m.getName()))
            .filter(StageReflection::hasConfigurableParamsOrIsStateless)
            .toList();

        if (candidates.isEmpty())
            throw new IllegalStateException(
                "No canonical of(...) factory on " + stageClass.getName() +
                " - expected one public static of(...) whose parameters are @Configurable, or a zero-arg of()"
            );

        if (candidates.size() > 1)
            throw new IllegalStateException(
                "Ambiguous of(...) factories on " + stageClass.getName() +
                " - found " + candidates.size() + " matching candidates; annotate exactly one"
            );

        Method m = candidates.getFirst();
        try {
            m.setAccessible(true);
        } catch (RuntimeException ignored) { }
        return m;
    }

    private static boolean hasConfigurableParamsOrIsStateless(@NotNull Method m) {
        Parameter[] params = m.getParameters();
        if (params.length == 0) return true;
        return Arrays.stream(params).allMatch(p -> p.isAnnotationPresent(Configurable.class));
    }

    private static @NotNull StageMetadata.Slot<?> slotOf(@NotNull Parameter parameter, @NotNull Reflection<?> reflection) {
        Configurable configurable = parameter.getAnnotation(Configurable.class);
        String paramName = parameter.getName();

        if (GENERATED_ARG_NAME.matcher(paramName).matches())
            throw new IllegalStateException(
                "Parameter name '" + paramName + "' on " + parameter.getDeclaringExecutable() +
                " looks like a generated argN - build with -parameters"
            );

        String wireKey = configurable.name().isEmpty() ? paramName : configurable.name();

        TypeResolution resolution = resolveParameter(parameter);
        FieldSpec<?> spec = new FieldSpec<>(
            wireKey,
            resolution.fieldType,
            configurable.label(),
            configurable.placeholder(),
            configurable.optional()
        );
        FieldAccessor<?> instanceField = reflection.getField(paramName);
        return new StageMetadata.Slot<>(spec, paramName, instanceField, resolution.adapter);
    }

    private static @NotNull TypeResolution resolveParameter(@NotNull Parameter parameter) {
        Type parameterized = parameter.getParameterizedType();
        Class<?> raw = rawType(parameterized);

        if (raw == String.class) return new TypeResolution(FieldSpec.Type.STRING, StageMetadata.Slot.IDENTITY);
        if (raw == int.class || raw == Integer.class) return new TypeResolution(FieldSpec.Type.INT, StageMetadata.Slot.IDENTITY);
        if (raw == long.class || raw == Long.class) return new TypeResolution(FieldSpec.Type.LONG, StageMetadata.Slot.IDENTITY);
        if (raw == double.class || raw == Double.class) return new TypeResolution(FieldSpec.Type.DOUBLE, StageMetadata.Slot.IDENTITY);
        if (raw == boolean.class || raw == Boolean.class) return new TypeResolution(FieldSpec.Type.BOOLEAN, StageMetadata.Slot.IDENTITY);
        if (DataType.class.isAssignableFrom(raw)) return new TypeResolution(FieldSpec.Type.DATA_TYPE, StageMetadata.Slot.IDENTITY);
        if (raw == Chain.class) return new TypeResolution(FieldSpec.Type.SUB_PIPELINE, StageMetadata.Slot.IDENTITY);
        if (raw == NamedChains.class) return new TypeResolution(FieldSpec.Type.SUB_PIPELINES_MAP, StageMetadata.Slot.IDENTITY);
        if (List.class.isAssignableFrom(raw) && listOfStages(parameterized))
            return new TypeResolution(FieldSpec.Type.SUB_PIPELINE, CHAIN_TO_STAGES);
        if (Map.class.isAssignableFrom(raw)) {
            if (typedChainValued(parameterized))
                return new TypeResolution(FieldSpec.Type.TYPED_SUB_PIPELINES_MAP, StageMetadata.Slot.IDENTITY);
            if (stringKeyedListOfStagesValued(parameterized))
                return new TypeResolution(FieldSpec.Type.SUB_PIPELINES_MAP, NAMED_CHAINS_TO_LISTS);
        }

        throw new IllegalStateException(
            "Cannot infer FieldSpec.Type for parameter '" + parameter.getName() + "' of type " +
            parameterized.getTypeName() + " on " + parameter.getDeclaringExecutable()
        );
    }

    private static @NotNull Class<?> rawType(@NotNull Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType pt) return (Class<?>) pt.getRawType();
        if (t instanceof WildcardType wt) {
            Type[] upper = wt.getUpperBounds();
            if (upper.length > 0) return rawType(upper[0]);
        }
        throw new IllegalStateException("Unsupported Type kind: " + t.getClass());
    }

    private static boolean listOfStages(@NotNull Type t) {
        if (!(t instanceof ParameterizedType pt)) return false;
        Type[] args = pt.getActualTypeArguments();
        if (args.length != 1) return false;
        return rawType(args[0]) == Stage.class;
    }

    private static boolean typedChainValued(@NotNull Type t) {
        if (!(t instanceof ParameterizedType pt)) return false;
        Type[] args = pt.getActualTypeArguments();
        return args.length == 2 && rawType(args[0]) == String.class && rawType(args[1]) == TypedChain.class;
    }

    private static boolean stringKeyedListOfStagesValued(@NotNull Type t) {
        if (!(t instanceof ParameterizedType pt)) return false;
        Type[] args = pt.getActualTypeArguments();
        if (args.length != 2) return false;
        if (rawType(args[0]) != String.class) return false;
        Type valueType = args[1];
        if (valueType instanceof WildcardType wt) {
            Type[] upper = wt.getUpperBounds();
            if (upper.length != 1) return false;
            valueType = upper[0];
        }
        if (!(valueType instanceof ParameterizedType valPt)) return false;
        if (!List.class.isAssignableFrom((Class<?>) valPt.getRawType())) return false;
        Type[] valArgs = valPt.getActualTypeArguments();
        return valArgs.length == 1 && rawType(valArgs[0]) == Stage.class;
    }

    private record TypeResolution(@NotNull FieldSpec.Type fieldType, @NotNull Function<Object, Object> adapter) {}

}

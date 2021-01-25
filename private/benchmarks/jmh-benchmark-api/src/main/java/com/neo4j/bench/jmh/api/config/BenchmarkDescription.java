/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.google.common.collect.Sets;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Describes one available benchmark (class that extends AbstractBenchmark),
 * including name, group, description, enabled status, thread safety, methods, and parameters.
 */
public class BenchmarkDescription
{

    /**
     * Combines BenchmarkDescriptions with the same className into a single BenchmarkDescription. Reverse operation of {@link BenchmarkDescription#explode()}
     * @implNote Imploding benchmarks and then exploding them again can lead to duplications.
     * @param benchmarkDescriptions BenchmarkDescriptions to implode
     * @return imploded BenchmarkDescriptions
     */
    static List<BenchmarkDescription> implode( Set<BenchmarkDescription> benchmarkDescriptions )
    {
        Map<String,List<BenchmarkDescription>> descriptionsByName =
                benchmarkDescriptions.stream().collect( groupingBy( BenchmarkDescription::className ) );

        return descriptionsByName.values()
                                 .stream()
                                 .map( BenchmarkDescription::implodeBenchmarkDescription )
                                 .collect( toList() );
    }

    /**
     * Implodes BenchmarkDescriptions with the same `className`
     * @param group List of BenchmarkDescription with the same className
     * @return imploded BenchmarkDescription
     */
    private static BenchmarkDescription implodeBenchmarkDescription( List<BenchmarkDescription> group )
    {
        Map<String,BenchmarkMethodDescription> implodedMethods = group
                .stream()
                .flatMap( bd -> bd.methods().stream() )
                .collect( toMap(
                        BenchmarkMethodDescription::name,
                        Function.identity(),
                        ( method1, method2 ) ->
                        {
                            if ( method1 == method2 )
                            {
                                return method1;
                            }
                            else
                            {
                                HashSet<Mode> combinedModes = new HashSet<>();
                                combinedModes.addAll( method1.modes() );
                                combinedModes.addAll( method2.modes() );

                                return new BenchmarkMethodDescription( method1.name(), combinedModes.toArray( new Mode[0] ) );
                            }
                        }
                ) );

        Map<String,BenchmarkParamDescription> implodedParameters = group
                .stream()
                .flatMap( bd1 -> bd1.parameters().values().stream() )
                .collect( groupingBy( BenchmarkParamDescription::name ) )
                .values()
                .stream()
                .map( parameterGroup -> new BenchmarkParamDescription(
                        parameterGroup.get( 0 ).name(),
                        parameterGroup.get( 0 ).allowedValues(),
                        parameterGroup.stream().flatMap( param -> param.values().stream() ).collect( toSet() )
                ) ).collect( toMap(
                        BenchmarkParamDescription::name,
                        Function.identity()
                ) );

        return new BenchmarkDescription(
                group.get( 0 ).className(),
                group.get( 0 ).group(),
                group.get( 0 ).isThreadSafe(),
                implodedMethods,
                implodedParameters,
                group.get( 0 ).description(),
                true
        );
    }

    private final String className;
    private final String group;
    private final boolean isThreadSafe;
    private final Map<String,BenchmarkMethodDescription> methods;
    private final Map<String,BenchmarkParamDescription> parameters;
    private final String description;
    private boolean isEnabled;

    public BenchmarkDescription(
            String className,
            String group,
            boolean isThreadSafe,
            Map<String,BenchmarkMethodDescription> methods,
            Map<String,BenchmarkParamDescription> parameters,
            String description,
            boolean isEnabled )
    {
        this.className = className;
        this.group = group;
        this.isThreadSafe = isThreadSafe;
        this.methods = methods;
        this.parameters = parameters;
        this.description = description;
        this.isEnabled = isEnabled;
    }

    /**
     * Attempt to construct full benchmark name from {@link BenchmarkDescription} instance.
     * This string is only used for error reporting.
     * It is not essential for the string to be identical to that returned by {@link Benchmark#name()}.
     * <p>
     * IMPORTANT: method assumes the {@link BenchmarkDescription} instance represents exactly one benchmark, e.g., after {@link BenchmarkDescription#explode()}.
     */
    public String guessSingleName()
    {
        if ( methods().size() != 1 )
        {
            throw new RuntimeException( "Benchmark description represents more than one benchmark\n" +
                                        "Expected one method but found: " + methods() );
        }
        BenchmarkMethodDescription method = methods().iterator().next();

        if ( method.modes().size() != 1 )
        {
            throw new RuntimeException( "Benchmark description represents more than one benchmark\n" +
                                        "Expected one mode but found: " + method.modes() );
        }
        Benchmark.Mode mode = BenchmarkDiscoveryUtils.toNativeMode( method.modes().iterator().next() );

        Map<String,String> params = parameters().values()
                                                .stream()
                                                .peek( paramDescription ->
                                                       {
                                                           if ( paramDescription.values().size() != 1 )
                                                           {
                                                               throw new RuntimeException(
                                                                       "Benchmark description represents more than one benchmark\n" +
                                                                       "Expected single values params but found: " + paramDescription );
                                                           }
                                                       } )
                                                .collect( toMap( BenchmarkParamDescription::name, p -> p.values().iterator().next() ) );

        try
        {
            String simpleName = Class.forName( className ).getSimpleName() + "." + method.name();
            return Benchmark.benchmarkFor( description(), simpleName, mode, params ).name();
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( format( "Was unable to create an instance of: %s", className ), e );
        }
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    public String className()
    {
        return className;
    }

    public String description()
    {
        return description;
    }

    public String group()
    {
        return group;
    }

    public boolean isThreadSafe()
    {
        return isThreadSafe;
    }

    public Map<String,BenchmarkParamDescription> parameters()
    {
        return parameters;
    }

    public Collection<BenchmarkMethodDescription> methods()
    {
        return methods.values();
    }

    String simpleName()
    {
        return className.substring( className.lastIndexOf( '.' ) + 1 );
    }

    public BenchmarkDescription copyRetainingMethods( String... methods )
    {
        Map<String,BenchmarkMethodDescription> retained = new HashMap<>();
        for ( String method : methods )
        {
            BenchmarkMethodDescription description = this.methods.get( method );
            retained.put( method, description );
        }
        return new BenchmarkDescription( className, group, isThreadSafe, retained, parameters, description, isEnabled );
    }

    public BenchmarkDescription copyAndEnable()
    {
        return new BenchmarkDescription(
                className,
                group,
                isThreadSafe,
                methods,
                parameters,
                description,
                true );
    }

    BenchmarkDescription copyWithConfig( BenchmarkConfigFileEntry benchmarkConfigEntry, Validation validation )
    {
        if ( !className.equals( benchmarkConfigEntry.name() ) )
        {
            throw new RuntimeException( format( "Cannot apply config for '%s' on benchmark '%s'",
                                                benchmarkConfigEntry.name(),
                                                className ) );
        }
        Map<String,BenchmarkParamDescription> newParameters = new HashMap<>( parameters );

        BiConsumer<String,Set<String>> updateParametersFun = ( paramName, newValues ) ->
        {
            if ( !parameters.containsKey( paramName ) )
            {
                validation.configuredParameterDoesNotExist( className, paramName );
            }
            else
            {
                BenchmarkParamDescription oldValues = parameters.get( paramName );
                for ( String newValue : newValues )
                {
                    if ( !oldValues.allowedValues().contains( newValue ) )
                    {
                        validation.configuredValueIsNotAllowed(
                                className,
                                paramName,
                                oldValues.allowedValues(),
                                newValue );
                    }
                }
                newParameters.put( paramName, oldValues.withValues( newValues ) );
            }
        };
        benchmarkConfigEntry.values().forEach( updateParametersFun );

        if ( benchmarkConfigEntry.isEnabled() )
        {
            newParameters.keySet().stream()
                         .filter( paramName ->
                                          newParameters.get( paramName ).values().isEmpty() )
                         .forEach( paramName ->
                                           validation.paramOfEnabledBenchmarkConfiguredWithNoValues( className, paramName ) );
        }
        return new BenchmarkDescription(
                className,
                group,
                isThreadSafe,
                methods,
                newParameters,
                description,
                benchmarkConfigEntry.isEnabled() );
    }

    public int executionCount( int threadCount )
    {
        if ( !isThreadSafe && threadCount != 1 )
        {
            return 0;
        }
        else
        {
            int parameterCombinations = 1;
            for ( BenchmarkParamDescription param : parameters.values() )
            {
                parameterCombinations *= param.values().size();
            }
            int executionCount = 0;
            for ( BenchmarkMethodDescription method : methods() )
            {
                executionCount += parameterCombinations * method.modes().size();
            }
            return executionCount;
        }
    }

    public int storeCount( List<String> contributingParameters )
    {
        int parameterCombinations = 1;
        for ( BenchmarkParamDescription param : parameters.values() )
        {
            if ( contributingParameters.contains( param.name() ) )
            {
                parameterCombinations *= param.values().size();
            }
        }
        return parameterCombinations;
    }

    static Set<Set<BenchmarkParamDescription>> explodeParameters( List<BenchmarkParamDescription> parameters )
    {
        if ( parameters.isEmpty() )
        {
            throw new RuntimeException( "This should never happen. Base case is when collection contains one element" );
        }
        else if ( parameters.size() == 1 )
        {
            return parameters.get( 0 ).explode().stream()
                             .map( Sets::newHashSet )
                             .collect( toSet() );
        }
        else
        {
            BenchmarkParamDescription head = parameters.get( 0 );
            List<BenchmarkParamDescription> tail = parameters.subList( 1, parameters.size() );

            Set<Set<BenchmarkParamDescription>> exploded = new HashSet<>();
            Set<Set<BenchmarkParamDescription>> explodedTail = explodeParameters( tail );

            for ( BenchmarkParamDescription headElement : head.explode() )
            {
                for ( Set<BenchmarkParamDescription> explodedTailElement : explodedTail )
                {
                    Set<BenchmarkParamDescription> explodedElement = new HashSet<>();
                    explodedElement.addAll( explodedTailElement );
                    explodedElement.add( headElement );
                    exploded.add( explodedElement );
                }
            }
            return exploded;
        }
    }

    public Set<BenchmarkDescription> explode()
    {
        Set<BenchmarkDescription> allCombinations = new HashSet<>();

        for ( BenchmarkMethodDescription method : methods.values().stream()
                                                         .flatMap( method -> method.explode().stream() )
                                                         .collect( toList() ) )
        {
            if ( parameters.isEmpty() )
            {
                HashMap<String,BenchmarkMethodDescription> methodForCombination = new HashMap<>();
                methodForCombination.put( method.name(), method );
                Map<String,BenchmarkParamDescription> emptyParameters = new HashMap<>();
                allCombinations.add(
                        new BenchmarkDescription(
                                className,
                                group,
                                isThreadSafe,
                                methodForCombination,
                                emptyParameters,
                                description,
                                isEnabled ) );
            }
            else
            {
                for ( Set<BenchmarkParamDescription> parameterCombination :
                        explodeParameters( newArrayList( parameters.values() ) ) )
                {
                    HashMap<String,BenchmarkMethodDescription> methodForCombination = new HashMap<>();
                    methodForCombination.put( method.name(), method );
                    Map<String,BenchmarkParamDescription> parametersForCombination =
                            parameterCombination.stream().collect( toMap( BenchmarkParamDescription::name,
                                                                          identity() ) );

                    allCombinations.add(
                            new BenchmarkDescription(
                                    className,
                                    group,
                                    isThreadSafe,
                                    methodForCombination,
                                    parametersForCombination,
                                    description,
                                    isEnabled ) );
                }
            }
        }
        return allCombinations;
    }

    @Override
    public String toString()
    {
        return format( "%s,\n" +
                       "\tenabled=%b,\n" +
                       "\tthread safe=%b,\n" +
                       "\tmethods=%s,\n" +
                       "\tparams=%s\n", className, isEnabled, isThreadSafe, methods, parameters );
    }

    // FACTORY METHODS

    public static BenchmarkDescription of( Class clazz, Validation validation, BenchmarksFinder benchmarksFinder )
    {
        Map<String,BenchmarkParamDescription> parameters = benchmarksFinder.getParamFieldsFor( clazz ).stream()
                                                                           .collect( toMap(
                                                                                   Field::getName,
                                                                                   field -> createBenchmarkParamDescriptionFor( field, validation, clazz ) ) );
        Map<String,BenchmarkMethodDescription> methods = benchmarksFinder.getBenchmarkMethodsFor( clazz ).stream()
                                                                         .map( method -> createBenchmarkMethodDescriptionFor( method,
                                                                                                                              validation,
                                                                                                                              benchmarksFinder ) )
                                                                         // distinct is needed because @Group benchmarks have multiple methods with same name
                                                                         // distinct is safe because every method of the same group should have the same mode
                                                                         .distinct()
                                                                         .collect( toMap( BenchmarkMethodDescription::name, identity() ) );
        return new BenchmarkDescription(
                clazz.getName(),
                BenchmarkDiscoveryUtils.benchmarkGroupFor( clazz ),
                BenchmarkDiscoveryUtils.isThreadSafe( clazz ),
                methods,
                parameters,
                BenchmarkDiscoveryUtils.descriptionFor( clazz ),
                BenchmarkDiscoveryUtils.isEnabled( clazz ) );
    }

    // HELPERS

    private static BenchmarkParamDescription createBenchmarkParamDescriptionFor( Field field, Validation validation, Class clazz )
    {
        Optional<ParamValues> paramValues = getAnnotationOrReport(
                ParamValues.class,
                field,
                identity(),
                () -> validation.missingParameterValue( field ) );

        String[] allowed = paramValues.map( ParamValues::allowed )
                                      .orElse( new String[0] );
        String[] base = paramValues.map( ParamValues::base )
                                   .orElse( new String[0] );

        Set<String> allowedValues = newHashSet( allowed );
        if ( allowedValues.size() != allowed.length )
        {
            validation.duplicateAllowedValue( clazz.getName(),
                                              field.getName(),
                                              allowed );
        }

        Set<String> defaultValues = newHashSet( base );
        if ( defaultValues.size() != base.length )
        {
            validation.duplicateBaseValue( clazz.getName(),
                                           field.getName(),
                                           base );
        }
        return new BenchmarkParamDescription( field.getName(),
                                              allowedValues,
                                              defaultValues );
    }

    private static BenchmarkMethodDescription createBenchmarkMethodDescriptionFor( Method benchmarkMethod, Validation validation,
                                                                                   BenchmarksFinder benchmarksFinder )
    {
        Optional<Mode[]> mode = getAnnotationOrReport(
                BenchmarkMode.class,
                benchmarkMethod,
                BenchmarkMode::value,
                () -> validation.missingBenchmarkMode( benchmarkMethod ) );

        return new BenchmarkMethodDescription(
                benchmarksFinder.benchmarkNameFor( benchmarkMethod ),
                mode.orElse( new Mode[0] ) );
    }

    private static <ANNOTATION extends Annotation, RETURN> Optional<RETURN> getAnnotationOrReport( Class<ANNOTATION> annotationClass,
                                                                                                   AccessibleObject object,
                                                                                                   Function<ANNOTATION,RETURN> valueFun,
                                                                                                   Runnable validationReporter )
    {
        ANNOTATION annotation = object.getAnnotation( annotationClass );
        if ( null == annotation )
        {
            validationReporter.run();
            return Optional.empty();
        }
        else
        {
            return Optional.of( valueFun.apply( annotation ) );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkDescription that = (BenchmarkDescription) o;
        return isThreadSafe == that.isThreadSafe &&
               isEnabled == that.isEnabled &&
               Objects.equals( className, that.className ) &&
               Objects.equals( group, that.group ) &&
               Objects.equals( methods, that.methods ) &&
               Objects.equals( parameters, that.parameters ) &&
               Objects.equals( description, that.description );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( className, group, isThreadSafe, methods, parameters, description, isEnabled );
    }
}

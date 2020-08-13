/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.google.common.reflect.ClassPath;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * This class discovers available benchmarks in a given package and extracts available benchmark methods and parameter fields and values.
 * It also provides utility functions to benchmark verification.
 */
public class BenchmarksFinder
{
    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages
     *
     * @param packageName the package (including subpackages) in which to find benchmark classes
     * @return a list of available benchmark classes
     */
    private static List<Class> benchmarkClasses( String packageName )
    {
        try
        {
            return ClassPath.from( ClassLoader.getSystemClassLoader() )
                            .getAllClasses()
                            .stream()
                            .filter( classInfo -> classInfo.getPackageName().startsWith( packageName ) )
                            .filter( classInfo -> !classInfo.getName().contains( ".jmh_generated." ) )
                            .map( ClassPath.ClassInfo::load )
                            .filter( clazz -> !Modifier.isAbstract( clazz.getModifiers() ) )
                            // ignore anonymous benchmark classes (awkwardly, they are sometimes created for testing purposes)
                            .filter( clazz -> !clazz.isAnonymousClass() )
                            // has benchmark method
                            .filter( BenchmarksFinder::isMaybeBenchmark )
                            .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error loading benchmark classes", e );
        }
    }

    /**
     * Tries to detect all classes that a user may think are a valid benchmark.
     * The intention is to catch as many user errors as possible.
     *
     * @param benchmark a benchmark class
     * @return true, if the class seems like a user intends it to be a runnable benchmark
     */
    private static boolean isMaybeBenchmark( Class<?> benchmark )
    {
        return BaseBenchmark.class.isAssignableFrom( benchmark ) ||
               Stream.of( benchmark.getMethods() ).anyMatch( m -> m.isAnnotationPresent( Benchmark.class ) ) ||
               Stream.of( benchmark.getFields() ).anyMatch( f -> f.isAnnotationPresent( Param.class ) );
    }

    private final Map<Class,List<Method>> benchmarkMethods;
    private final Map<Class,List<Field>> benchmarkParamFields;
    private final Map<Class,List<Field>> benchmarkParamValueFields;

    /**
     * Creates a new instance of Annotations for the given package
     *
     * @param packageName the package (including subpackages) in which to find benchmark classes
     */
    public BenchmarksFinder( String packageName )
    {
        List<Class> benchmarks = benchmarkClasses( packageName );

        this.benchmarkMethods = benchmarks.stream()
                                          .collect( toMap( identity(),
                                                           b -> Stream.of( b.getMethods() )
                                                                      .filter( m -> m.isAnnotationPresent( Benchmark.class ) )
                                                                      .collect( toList() ) ) );

        this.benchmarkParamFields = benchmarks.stream()
                                              .collect( toMap( identity(),
                                                               b -> Stream.of( b.getDeclaredFields() )
                                                                          .filter( field -> field.isAnnotationPresent( Param.class ) )
                                                                          .collect( toList() ) ) );

        this.benchmarkParamValueFields = benchmarks.stream()
                                                   .collect( toMap( identity(),
                                                                    b -> Stream.of( b.getDeclaredFields() )
                                                                               .filter( field -> field.isAnnotationPresent( ParamValues.class ) )
                                                                               .collect( toList() ) ) );
    }

    List<Method> getBenchmarkMethodsFor( Class benchmarkClass )
    {
        return benchmarkMethods.getOrDefault( benchmarkClass, emptyList() );
    }

    public Set<Class> getBenchmarks()
    {
        return benchmarkMethods.keySet();
    }

    List<Field> getParamFieldsFor( Class clazz )
    {
        return Stream.of( clazz.getDeclaredFields() )
                     .filter( field -> field.isAnnotationPresent( Param.class ) )
                     .collect( toList() );
    }

    /**
     * When multiple @Benchmark methods are part of the same @Group they should all have the same name,
     * the name of the group. This is both how JMH identifies methods, and how the results store should identify them.
     *
     * @param method the method for which to get the benchmark name
     * @return benchmark name
     */
    String benchmarkNameFor( Method method )
    {
        return method.isAnnotationPresent( Group.class )
               ? method.getAnnotation( Group.class ).value()
               : method.getName();
    }

    boolean hasBenchmark( String className )
    {
        return benchmarkMethods.keySet().stream().anyMatch( b -> b.getName().equals( className ) );
    }

    private List<Field> getParamValueFields()
    {
        return benchmarkParamValueFields.values().stream().flatMap( List::stream ).collect( toList() );
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------------ Validation Related ------------------------------------
    // --------------------------------------------------------------------------------------------

    public BenchmarksValidator.BenchmarkValidationResult validate()
    {
        return new BenchmarksValidator( this ).validate();
    }

    Map<Class,List<Method>> getBenchmarkMethods()
    {
        return benchmarkMethods;
    }

    // NOTE: this is a sanity check, to make sure old code is updated to remove the class name prefixes that used to be necessary
    List<Field> getParamFieldsWithClassNamePrefix()
    {
        return getParamFields().stream()
                               .filter( field -> field.getName().startsWith(
                                       field.getDeclaringClass().getSimpleName() ) )
                               .collect( toList() );
    }

    List<Field> assignedParamFields()
    {
        return getParamFields().stream()
                               .filter( field -> field.getAnnotation( Param.class ).value().length != 0 )
                               .collect( toList() );
    }

    List<Field> paramFieldsWithoutParamValue()
    {
        return getParamFields().stream()
                               .filter( field -> !Modifier.isAbstract( field.getDeclaringClass().getModifiers() ) )
                               .filter( field -> !field.isAnnotationPresent( ParamValues.class ) )
                               .collect( toList() );
    }

    List<Method> benchmarkMethodsWithoutModeAnnotation()
    {
        return getBenchmarks().stream()
                              .flatMap( clazz -> getBenchmarkMethodsFor( clazz ).stream() )
                              .filter( method -> !method.isAnnotationPresent( BenchmarkMode.class ) )
                              .collect( toList() );
    }

    /**
     * Benchmarks that use @TearDown in a way that would not work with the/our JMH life cycle
     */
    List<Class> classesWithTearDownMethod()
    {
        return getBenchmarks().stream()
                              .filter( this::hasTearDownMethod )
                              .collect( toList() );
    }

    /**
     * Benchmarks that use @Setup in a way that would not work with the/our JMH life cycle
     */
    List<Class> classesWithSetupMethod()
    {
        return getBenchmarks().stream()
                              .filter( this::hasSetupMethod )
                              .collect( toList() );
    }

    private List<Field> getParamFields()
    {
        return benchmarkParamFields.values().stream().flatMap( List::stream ).collect( toList() );
    }

    /**
     * Check if benchmark uses @Setup in a way that would not work with the/our JMH life cycle
     */
    private boolean hasSetupMethod( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getMethods() )
                     .filter( method -> !BaseBenchmark.class.equals( method.getDeclaringClass() ) )
                     .filter( method -> method.isAnnotationPresent( Setup.class ) )
                     .anyMatch( method -> method.getAnnotation( Setup.class ).value().equals( Level.Trial ) );
    }

    /**
     * Check if benchmark uses @TearDown in a way that would not work with the/our JMH life cycle
     */
    private boolean hasTearDownMethod( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getMethods() )
                     .filter( method -> !BaseBenchmark.class.equals( method.getDeclaringClass() ) )
                     .filter( method -> method.isAnnotationPresent( TearDown.class ) )
                     .anyMatch( method -> method.getAnnotation( TearDown.class ).value().equals( Level.Trial ) );
    }

    List<Class> classesThatDoNotExtendBaseBenchmark()
    {
        return getBenchmarks().stream()
                              .filter( benchmarkClass -> !BaseBenchmark.class.isAssignableFrom( benchmarkClass ) )
                              .collect( toList() );
    }

    Map<Field,Set<String>> erroneousBaseValues()
    {
        Map<Field,Set<String>> erroneousFieldValues = new HashMap<>();
        for ( Field field : getParamValueFields() )
        {
            Set<String> allowed = newHashSet( field.getAnnotation( ParamValues.class ).allowed() );
            Set<String> base = newHashSet( field.getAnnotation( ParamValues.class ).base() );
            if ( !allowed.containsAll( base ) )
            {
                base.removeAll( allowed );
                erroneousFieldValues.put( field, base );
            }
        }
        return erroneousFieldValues;
    }

    List<Field> paramValueFieldsWithEmptyBase()
    {
        return getParamValueFields().stream()
                                    .filter( field -> field.getAnnotation( ParamValues.class ).base().length == 0 )
                                    .collect( toList() );
    }

    List<Field> paramValueFieldsWithEmptyAllowed()
    {
        return getParamValueFields().stream()
                                    .filter( field -> field.getAnnotation( ParamValues.class ).allowed().length == 0 )
                                    .collect( toList() );
    }

    List<Field> paramValueFieldsWithoutParam()
    {
        return getParamValueFields().stream()
                                    .filter( field -> !field.isAnnotationPresent( Param.class ) )
                                    .collect( toList() );
    }
}

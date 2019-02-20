/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.google.common.reflect.ClassPath;
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;
import com.neo4j.bench.micro.benchmarks.Neo4jBenchmark;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class Annotations
{
    // Scans all classes accessible from the context class loader which belong to the given package and subpackages
    static List<Class> benchmarkClasses()
    {
        try
        {
            return ClassPath.from( ClassLoader.getSystemClassLoader() )
                            .getAllClasses()
                            .stream()
                            .filter( classInfo -> classInfo.getPackageName().startsWith( "com.neo4j.bench" ) )
                            .filter( classInfo -> !classInfo.getName().contains( ".generated." ) )
                            .map( ClassPath.ClassInfo::load )
                            .filter( clazz -> !Modifier.isAbstract( clazz.getModifiers() ) )
                            .filter( Annotations::hasBenchmarkMethod )
                            .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error loading benchmark classes", e );
        }
    }

    public static Class benchmarkClassForName( String benchmarkClassName )
    {
        try
        {
            Class benchmarkClass = Class.forName( benchmarkClassName );
            if ( !extendsBaseBenchmark( benchmarkClass ) )
            {
                throw new RuntimeException( format( "Class with name %s is not a benchmark", benchmarkClassName ) );
            }
            return benchmarkClass;
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( "Unable to get class for: " + benchmarkClassName );
        }
    }

    public static String descriptionFor( String benchmarkClassName )
    {
        return descriptionFor( benchmarkClassForName( benchmarkClassName ) );
    }

    public static String descriptionFor( Class<? extends Neo4jBenchmark> benchmarkClass )
    {
        try
        {
            Neo4jBenchmark abstractBenchmark = benchmarkClass.newInstance();
            return abstractBenchmark.description();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving 'description' from: " + benchmarkClass.getName(), e );
        }
    }

    public static boolean isThreadSafe( String benchmarkClassName )
    {
        return isThreadSafe( benchmarkClassForName( benchmarkClassName ) );
    }

    public static boolean isThreadSafe( Class<? extends Neo4jBenchmark> benchmarkClass )
    {
        try
        {
            Neo4jBenchmark abstractBenchmark = benchmarkClass.newInstance();
            return abstractBenchmark.isThreadSafe();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving 'thread safe' from: " + benchmarkClass.getName(), e );
        }
    }

    public static String benchmarkGroupFor( String benchmarkClassName )
    {
        return benchmarkGroupFor( benchmarkClassForName( benchmarkClassName ) );
    }

    static String benchmarkGroupFor( Class<? extends Neo4jBenchmark> benchmarkClass )
    {
        try
        {
            Neo4jBenchmark abstractBenchmark = benchmarkClass.newInstance();
            return abstractBenchmark.benchmarkGroup();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving 'benchmark group' from: " + benchmarkClass.getName(), e );
        }
    }

    private static List<Class> benchmarkClassesWithParamFields( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .filter( Annotations::hasParamField )
                               .collect( toList() );
    }

    private static List<Field> paramFieldsFor( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .flatMap( Annotations::paramFieldsFor )
                               .collect( toList() );
    }

    static Stream<Field> paramFieldsFor( Class clazz )
    {
        return Stream.of( clazz.getDeclaredFields() )
                     .filter( field -> field.isAnnotationPresent( Param.class ) );
    }

    private static List<Field> paramValueFieldsFor( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .flatMap( clazz -> paramValueFieldsFor( clazz ).stream() )
                               .collect( toList() );
    }

    private static List<Field> paramValueFieldsFor( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getDeclaredFields() )
                     .filter( field -> field.isAnnotationPresent( ParamValues.class ) )
                     .collect( toList() );
    }

    static boolean isEnabled( Class benchmarkClass )
    {
        return !benchmarkClass.isAnnotationPresent( BenchmarkEnabled.class ) ||
               ((BenchmarkEnabled) benchmarkClass.getAnnotation( BenchmarkEnabled.class )).value();
    }

    private static List<Field> paramFieldsWithoutClassNamePrefix( List<Class> benchmarkClasses )
    {
        return paramFieldsFor( benchmarkClasses ).stream()
                                                 .filter( field -> !field.getName().startsWith( field.getDeclaringClass().getSimpleName() + "_" ) )
                                                 .collect( toList() );
    }

    private static List<Field> assignedParamFields( List<Class> benchmarkClasses )
    {
        return paramFieldsFor( benchmarkClasses ).stream()
                                                 .filter( field -> field.getAnnotation( Param.class ).value().length != 0 )
                                                 .collect( toList() );
    }

    private static List<Field> paramFieldsWithoutParamValue( List<Class> benchmarkClasses )
    {
        return paramFieldsFor( benchmarkClasses ).stream()
                                                 .filter( field -> !field.isAnnotationPresent( ParamValues.class ) )
                                                 .collect( toList() );
    }

    private static List<Field> paramValueFieldsWithoutParam( List<Class> benchmarkClasses )
    {
        return paramValueFieldsFor( benchmarkClasses ).stream()
                                                      .filter( field -> !field.isAnnotationPresent( Param.class ) )
                                                      .collect( toList() );
    }

    private static List<Field> paramValueFieldsWithEmptyAllowed( List<Class> benchmarkClasses )
    {
        return paramValueFieldsFor( benchmarkClasses ).stream()
                                                      .filter( field -> field.getAnnotation( ParamValues.class ).allowed().length == 0 )
                                                      .collect( toList() );
    }

    private static List<Field> paramValueFieldsWithEmptyBase( List<Class> benchmarkClasses )
    {
        return paramValueFieldsFor( benchmarkClasses ).stream()
                                                      .filter( field -> field.getAnnotation( ParamValues.class ).base().length == 0 )
                                                      .collect( toList() );
    }

    private static Map<Field,Set<String>> erroneousBaseValues( List<Class> benchmarkClasses )
    {
        Map<Field,Set<String>> erroneousFieldValues = new HashMap<>();
        for ( Field field : paramValueFieldsFor( benchmarkClasses ) )
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

    private static Set<Class> classesWithPartiallyFilledBaseValues( List<Class> benchmarkClasses )
    {
        Set<Class> erroneousClasses = new HashSet<>();

        for ( Class clazz : benchmarkClassesWithParamFields( benchmarkClasses ) )
        {
            Set<String> emptyBaseFields = new HashSet<>();
            Set<String> nonEmptyBaseFields = new HashSet<>();
            for ( Field field : paramValueFieldsFor( clazz ) )
            {
                if ( field.getAnnotation( ParamValues.class ).base().length == 0 )
                {
                    emptyBaseFields.add( field.getName() );
                }
                else
                {
                    nonEmptyBaseFields.add( field.getName() );
                }
            }
            if ( emptyBaseFields.isEmpty() == nonEmptyBaseFields.isEmpty() )
            {
                erroneousClasses.add( clazz );
            }
        }

        return erroneousClasses;
    }

    private static List<Class> classesThatDoNotExtendBaseBenchmark( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .filter( benchmarkClass -> !extendsBaseBenchmark( benchmarkClass ) )
                               .collect( toList() );
    }

    private static List<Method> benchmarkMethodsWithoutModeAnnotation( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .flatMap( Annotations::benchmarkMethodsFor )
                               .filter( method -> !Annotations.hasModeAnnotation( method ) )
                               .collect( toList() );
    }

    private static boolean hasModeAnnotation( Method benchmarkMethod )
    {
        return benchmarkMethod.isAnnotationPresent( BenchmarkMode.class );
    }

    private static boolean hasBenchmarkMethod( Class clazz )
    {
        return benchmarkMethodsFor( clazz ).count() > 0;
    }

    static Stream<Method> benchmarkMethodsFor( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getMethods() ).filter( m -> m.isAnnotationPresent( Benchmark.class ) );
    }

    /**
     * Benchmarks that use @Setup in a way that would not work with the/our JMH life cycle
     */
    private static List<Class> classesWithSetupMethod( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .filter( Annotations::hasSetupMethod )
                               .collect( toList() );
    }

    /**
     * Benchmarks that use @TearDown in a way that would not work with the/our JMH life cycle
     */
    private static List<Class> classesWithTearDownMethod( List<Class> benchmarkClasses )
    {
        return benchmarkClasses.stream()
                               .filter( Annotations::hasTearDownMethod )
                               .collect( toList() );
    }

    /**
     * Check if benchmark uses @Setup in a way that would not work with the/our JMH life cycle
     */
    private static boolean hasSetupMethod( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getMethods() )
                     .filter( method -> !isBaseBenchmark( method.getDeclaringClass() ) )
                     .filter( method -> method.isAnnotationPresent( Setup.class ) )
                     .anyMatch( method -> method.getAnnotation( Setup.class ).value().equals( Level.Trial ) );
    }

    /**
     * Check if benchmark uses @TearDown in a way that would not work with the/our JMH life cycle
     */
    private static boolean hasTearDownMethod( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getMethods() )
                     .filter( method -> !isBaseBenchmark( method.getDeclaringClass() ) )
                     .filter( method -> method.isAnnotationPresent( TearDown.class ) )
                     .anyMatch( method -> method.getAnnotation( TearDown.class ).value().equals( Level.Trial ) );
    }

    private static boolean extendsBaseBenchmark( Class benchmarkClass )
    {
        return BaseRegularBenchmark.class.isAssignableFrom( benchmarkClass ) ||
               BaseDatabaseBenchmark.class.isAssignableFrom( benchmarkClass );
    }

    private static boolean isBaseBenchmark( Class benchmarkClass )
    {
        return BaseRegularBenchmark.class.equals( benchmarkClass ) ||
               BaseDatabaseBenchmark.class.equals( benchmarkClass );
    }

    /**
     * When multiple @Benchmark methods are part of the same @Group they should all have the same name,
     * the name of the group. This is both how JMH identifies methods, and how the results store should identify them.
     *
     * @param method
     * @return benchmark name
     */
    static String benchmarkNameFor( Method method )
    {
        return method.isAnnotationPresent( Group.class )
               ? method.getAnnotation( Group.class ).value()
               : method.getName();
    }

    private static boolean hasParamField( Class benchmarkClass )
    {
        return Stream.of( benchmarkClass.getFields() )
                     .anyMatch( field -> field.isAnnotationPresent( ParamValues.class ) );
    }

    private final List<Class> benchmarks;
    private final List<Field> paramFields;
    private final List<Field> paramFieldsWithoutClassNamePrefix;
    private final List<Field> assignedParamFields;
    private final List<Field> paramFieldsWithoutParamValue;
    private final List<Field> paramValueFieldsWithoutParam;
    private final List<Field> paramValueFieldsWithEmptyAllowed;
    private final List<Field> paramValueFieldsWithEmptyBase;
    private final Map<Field,Set<String>> erroneousBaseValues;
    private final Set<Class> classesWithPartiallyFilledBaseValues;
    private final List<Class> classesThatDoNotExtendBaseBenchmark;
    private final List<Class> classesWithSetupMethod;
    private final List<Class> classesWithTearDownMethod;
    private final List<Method> benchmarkMethodsWithoutModeAnnotation;

    public Annotations()
    {
        this.benchmarks = benchmarkClasses();
        this.paramFields = paramFieldsFor( benchmarks );
        this.paramFieldsWithoutClassNamePrefix = paramFieldsWithoutClassNamePrefix( benchmarks );
        this.assignedParamFields = assignedParamFields( benchmarks );
        this.paramFieldsWithoutParamValue = paramFieldsWithoutParamValue( benchmarks );
        this.paramValueFieldsWithoutParam = paramValueFieldsWithoutParam( benchmarks );
        this.paramValueFieldsWithEmptyAllowed = paramValueFieldsWithEmptyAllowed( benchmarks );
        this.paramValueFieldsWithEmptyBase = paramValueFieldsWithEmptyBase( benchmarks );
        this.erroneousBaseValues = erroneousBaseValues( benchmarks );
        this.classesWithPartiallyFilledBaseValues = classesWithPartiallyFilledBaseValues( benchmarks );
        this.classesThatDoNotExtendBaseBenchmark = classesThatDoNotExtendBaseBenchmark( benchmarks );
        this.classesWithSetupMethod = classesWithSetupMethod( benchmarks );
        this.classesWithTearDownMethod = classesWithTearDownMethod( benchmarks );
        this.benchmarkMethodsWithoutModeAnnotation = benchmarkMethodsWithoutModeAnnotation( benchmarks );
    }

    private boolean allBenchmarksExtendBaseBenchmark()
    {
        return classesThatDoNotExtendBaseBenchmark.isEmpty();
    }

    private boolean hasAtLeastOneBenchmarkClass()
    {
        // Sanity check that reflection code finds benchmark classes, if it finds one class it probably finds them all
        return benchmarks.size() > 0;
    }

    private boolean hasAtLeastOneParamFieldInBenchmarkClasses()
    {
        // Sanity check that reflection code actually finds fields, if it finds one class it probably finds them all
        return paramFields.size() > 0;
    }

    private boolean hasClassNamePrefixOnAllParamFieldNames()
    {
        // JMH options builder does not allow for setting param:value pairs on individual classes, only globally
        // To get around that this benchmark suite prepends benchmark class names onto the param names of those classes
        // This test makes sure this policy is being followed
        return paramFieldsWithoutClassNamePrefix.size() == 0;
    }

    private boolean hasEmptyValueOnAllParamFields()
    {
        // JMH options builder does not allow for setting param:value pairs on individual classes, only globally
        // To get around that this benchmark suite prepends benchmark class names onto the param names of those classes
        // This test makes sure this policy is being followed
        return assignedParamFields.size() == 0;
    }

    private boolean hasParamValueOnAllParamFields()
    {
        return paramFieldsWithoutParamValue.size() == 0;
    }

    private boolean hasParamOnAllParamValueFields()
    {
        return paramValueFieldsWithoutParam.size() == 0;
    }

    private boolean hasNonEmptyAllowedOnAllParamValueFields()
    {
        return paramValueFieldsWithEmptyAllowed.size() == 0;
    }

    private boolean hasNonEmptyBaseOnAllParamValueFields()
    {
        return paramValueFieldsWithEmptyBase.size() == 0;
    }

    private boolean isBaseValuesSubsetsOfAllowed()
    {
        return erroneousBaseValues.isEmpty();
    }

    private boolean isBenchmarkClassesCorrectlyEnabled()
    {
        return classesWithPartiallyFilledBaseValues.isEmpty();
    }

    private boolean noBenchmarkHasInvalidSetup()
    {
        return classesWithSetupMethod.isEmpty();
    }

    private boolean noBenchmarkHasInvalidTearDown()
    {
        return classesWithTearDownMethod.isEmpty();
    }

    private boolean allBenchmarkMethodsHaveModeAnnotation()
    {
        return benchmarkMethodsWithoutModeAnnotation.isEmpty();
    }

    public AnnotationsValidationResult validate()
    {
        boolean valid = hasAtLeastOneBenchmarkClass() &&
                        hasAtLeastOneParamFieldInBenchmarkClasses() &&
                        hasClassNamePrefixOnAllParamFieldNames() &&
                        hasEmptyValueOnAllParamFields() &&
                        hasParamValueOnAllParamFields() &&
                        hasParamOnAllParamValueFields() &&
                        hasNonEmptyAllowedOnAllParamValueFields() &&
                        hasNonEmptyBaseOnAllParamValueFields() &&
                        isBaseValuesSubsetsOfAllowed() &&
                        isBenchmarkClassesCorrectlyEnabled() &&
                        noBenchmarkHasInvalidSetup() &&
                        noBenchmarkHasInvalidTearDown() &&
                        allBenchmarkMethodsHaveModeAnnotation();
        String validationMessage = valid ? "Validation Passed" : validationErrors();
        return new AnnotationsValidationResult( valid, validationMessage );
    }

    private String validationErrors()
    {
        final StringBuilder sb = new StringBuilder( "Validation Failed\n" );
        if ( !hasAtLeastOneBenchmarkClass() )
        {
            sb.append( "\t" ).append( "* No benchmark classes found\n" );
        }
        if ( !hasAtLeastOneParamFieldInBenchmarkClasses() )
        {
            sb.append( "\t" ).append( "* No Param fields found\n" );
        }
        if ( !hasClassNamePrefixOnAllParamFieldNames() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( Param.class.getSimpleName() )
                    .append( " fields lack class name prefix:\n" );
            paramFieldsWithoutClassNamePrefix.forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getName() )
                    .append( " in " ).append( field.getDeclaringClass().getName() )
                    .append( " should have prefix: " ).append( field.getDeclaringClass().getSimpleName() ).append( "_" )
                    .append( "\n" )
            );
        }
        if ( !hasEmptyValueOnAllParamFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( Param.class.getSimpleName() )
                    .append( " annotations should have no value, but these fields do:\n" );
            assignedParamFields.forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasParamValueOnAllParamFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( Param.class.getSimpleName() )
                    .append( " fields should also have @" ).append( ParamValues.class.getSimpleName() )
                    .append( " annotation. These do not:\n" );
            paramFieldsWithoutParamValue.forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasParamOnAllParamValueFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " fields should have @" ).append( Param.class.getSimpleName() )
                    .append( " annotation. These do not:\n" );
            paramValueFieldsWithoutParam.forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasNonEmptyAllowedOnAllParamValueFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " annotations should have non-empty 'allowed' field. These do not:\n" );
            paramValueFieldsWithEmptyAllowed.forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasNonEmptyBaseOnAllParamValueFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " annotations should have non-empty 'base' field. These do not:\n" );
            paramValueFieldsWithEmptyBase.forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !isBaseValuesSubsetsOfAllowed() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " 'base' & 'extended' value must be subset of 'allowed'. These are not:\n" );
            erroneousBaseValues.entrySet().forEach( entry -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( entry.getKey().getDeclaringClass().getName() ).append( "." )
                    .append( entry.getKey().getName() )
                    .append( " has illegal 'base' values " ).append( entry.getValue() )
                    .append( "\n" )
            );
        }
        if ( !isBenchmarkClassesCorrectlyEnabled() )
        {
            sb
                    .append( "\t" ).append( "* Classes should have all 'base'/'extended' fields enabled," )
                    .append( "or all disabled. These classes have mixed (enabled AND disabled) fields:\n" );
            classesWithPartiallyFilledBaseValues.forEach( clazz -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( clazz.getName() ).append( " has mixed (enabled AND disabled) 'base' values" )
                    .append( "\n" )
            );
        }
        if ( !allBenchmarksExtendBaseBenchmark() )
        {
            sb
                    .append( "\t" ).append( "* Classes should extend base benchmarks. These classes do not:\n" );
            classesThatDoNotExtendBaseBenchmark.forEach( clazz -> sb
                    .append( "\t\t" ).append( "> " ).append( clazz.getName() ).append( "\n" )
            );
        }
        if ( !noBenchmarkHasInvalidSetup() )
        {
            sb
                    .append( "\t" ).append( "* Benchmarks should not have methods with @" )
                    .append( Setup.class.getName() ).append( "(" ).append( Level.Trial ).append( "), but these benchmarks do: \n " );
            classesWithSetupMethod.forEach( benchmark -> sb
                    .append( "\t\t" ).append( "> " ).append( benchmark.getName() ).append( "\n" )
            );
        }
        if ( !noBenchmarkHasInvalidTearDown() )
        {
            sb
                    .append( "\t" ).append( "* Benchmarks should not have methods with @" )
                    .append( TearDown.class.getName() ).append( "(" ).append( Level.Trial ).append( "), but these benchmarks do: \n " );
            classesWithTearDownMethod.forEach( benchmark -> sb
                    .append( "\t\t" ).append( "> " ).append( benchmark.getName() ).append( "\n" )
            );
        }
        if ( !allBenchmarkMethodsHaveModeAnnotation() )
        {
            sb
                    .append( "\t" ).append( "* Benchmark methods should be annotated with " )
                    .append( BenchmarkMode.class.getName() ).append( ", these methods do not:\n" );
            benchmarkMethodsWithoutModeAnnotation.forEach( method -> sb
                    .append( "\t\t" ).append( "> " ).append( method.getDeclaringClass().getName() ).append( "." )
                    .append( method.getName() ).append( "\n" )
            );
        }
        return sb.toString();
    }

    public static class AnnotationsValidationResult
    {
        private final boolean valid;
        private final String message;

        AnnotationsValidationResult( boolean valid, String message )
        {
            this.valid = valid;
            this.message = message;
        }

        public boolean isValid()
        {
            return valid;
        }

        public String message()
        {
            return message;
        }
    }
}

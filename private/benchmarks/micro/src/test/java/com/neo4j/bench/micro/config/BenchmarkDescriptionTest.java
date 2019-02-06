/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.neo4j.bench.micro.benchmarks.test_only.DuplicateAllowedBenchmark;
import com.neo4j.bench.micro.benchmarks.test_only.DuplicateBaseBenchmark;
import com.neo4j.bench.micro.benchmarks.test_only.ValidDisabledBenchmark;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark1;
import org.junit.Test;
import org.openjdk.jmh.annotations.Mode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.micro.TestUtils.map;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_BENCHMARK_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_PARAMETER_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_VALUE_IS_NOT_ALLOWED;
import static com.neo4j.bench.micro.config.Validation.ValidationError.DUPLICATE_ALLOWED_VALUE;
import static com.neo4j.bench.micro.config.Validation.ValidationError.DUPLICATE_BASE_VALUE;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Mode.SampleTime;
import static org.openjdk.jmh.annotations.Mode.Throughput;

public class BenchmarkDescriptionTest
{
    private static final Map<String,BenchmarkMethodDescription> METHODS = Collections.emptyMap();
    private String group = "regular";

    @Test
    public void shouldConstructFromValidExample() throws ClassNotFoundException
    {
        // when
        Validation validation = new Validation();
        Class benchmarkClass = ValidEnabledBenchmark1.class;
        String name = benchmarkClass.getName();
        BenchmarkDescription benchDesc = BenchmarkDescription.of( benchmarkClass, validation );

        // then
        assertThat( benchDesc.className(), equalTo( name ) );
        assertThat( benchDesc.group(), equalTo( "Example" ) );
        assertThat( benchDesc.description(), equalTo( benchmarkClass.getSimpleName() ) );
        assertThat( benchDesc.parameters().size(), equalTo( 2 ) );
        assertThat( benchDesc.isThreadSafe(), equalTo( false ) );
        assertThat( benchDesc.executionCount( 1 ), equalTo( 6 ) );
        assertThat( benchDesc.isEnabled(), equalTo( true ) );

        assertThat( benchDesc.parameters().get( "number" ).allowedValues(), equalTo( newHashSet( "1", "2" ) ) );
        assertThat( benchDesc.parameters().get( "number" ).values(), equalTo( newHashSet( "1" ) ) );
        assertThat( benchDesc.parameters().get( "number" ).valuesArray(), equalTo( new String[]{"1"} ) );

        assertThat( benchDesc.parameters().get( "string" ).allowedValues(), equalTo( newHashSet( "a", "b" ) ) );
        assertThat( benchDesc.parameters().get( "string" ).values(), equalTo( newHashSet( "a", "b" ) ) );
        assertThat( benchDesc.parameters().get( "string" ).valuesArray(), equalTo( new String[]{"a", "b"} ) );

        assertThat( newHashSet( benchDesc.methods() ), equalTo( newHashSet(
                new BenchmarkMethodDescription( "methodTwo", new Mode[]{Throughput, AverageTime} ),
                new BenchmarkMethodDescription( "methodOne", new Mode[]{SampleTime} ) ) ) );

        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldPassValidationWhenSettingAllParametersOnDisabledBenchmark()
    {
        // given
        Validation validation = new Validation();
        Class benchmarkClass = ValidDisabledBenchmark.class;
        String benchName = benchmarkClass.getName();
        BenchmarkDescription benchDesc = BenchmarkDescription.of( benchmarkClass, validation );

        assertThat( benchDesc.className(), equalTo( benchName ) );
        assertThat( benchDesc.group(), equalTo( "Example" ) );
        assertThat( benchDesc.description(), equalTo( benchmarkClass.getSimpleName() ) );
        assertThat( benchDesc.parameters().size(), equalTo( 2 ) );
        assertThat( benchDesc.isThreadSafe(), equalTo( false ) );
        assertThat( benchDesc.executionCount( 1 ), equalTo( 1 ) );
        assertThat( benchDesc.isEnabled(), equalTo( false ) );

        assertThat( benchDesc.parameters().get( "param1" ).allowedValues(), equalTo( newHashSet( "1", "2" ) ) );
        assertThat( benchDesc.parameters().get( "param1" ).values(), equalTo( newHashSet( "1" ) ) );
        assertThat( benchDesc.parameters().get( "param1" ).valuesArray(), equalTo( new String[]{"1"} ) );

        assertThat( benchDesc.parameters().get( "param2" ).allowedValues(), equalTo( newHashSet( "a", "b" ) ) );
        assertThat( benchDesc.parameters().get( "param2" ).values(), equalTo( newHashSet( "a" ) ) );
        assertThat( benchDesc.parameters().get( "param2" ).valuesArray(), equalTo( new String[]{"a"} ) );

        assertThat( newHashSet( benchDesc.methods() ),
                equalTo( newHashSet( new BenchmarkMethodDescription( "method", new Mode[]{SampleTime} ) ) ) );

        assertTrue( validation.report(), validation.isValid() );

        // when
        benchDesc = benchDesc
                .copyWithConfig(
                        new BenchmarkConfigFileEntry(
                                benchName,
                                false,
                                map( "param1", newHashSet( "1" ) ) ),
                        validation )
                .copyWithConfig(
                        new BenchmarkConfigFileEntry(
                                benchName,
                                false,
                                map( "param2", newHashSet( "a" ) ) ),
                        validation );

        // then
        assertThat( benchDesc.className(), equalTo( benchName ) );
        assertThat( benchDesc.group(), equalTo( "Example" ) );
        assertThat( benchDesc.description(), equalTo( benchmarkClass.getSimpleName() ) );
        assertThat( benchDesc.parameters().size(), equalTo( 2 ) );
        assertThat( benchDesc.isThreadSafe(), equalTo( false ) );
        assertThat( benchDesc.executionCount( 1 ), equalTo( 1 ) );
        assertThat( benchDesc.isEnabled(), equalTo( false ) );

        assertThat( benchDesc.parameters().get( "param1" ).allowedValues(), equalTo( newHashSet( "1", "2" ) ) );
        assertThat( benchDesc.parameters().get( "param1" ).values(), equalTo( newHashSet( "1" ) ) );
        assertThat( benchDesc.parameters().get( "param1" ).valuesArray(), equalTo( new String[]{"1"} ) );

        assertThat( benchDesc.parameters().get( "param2" ).allowedValues(), equalTo( newHashSet( "a", "b" ) ) );
        assertThat( benchDesc.parameters().get( "param2" ).values(), equalTo( newHashSet( "a" ) ) );
        assertThat( benchDesc.parameters().get( "param2" ).valuesArray(), equalTo( new String[]{"a"} ) );

        assertThat( newHashSet( benchDesc.methods() ),
                equalTo( newHashSet( new BenchmarkMethodDescription( "method", new Mode[]{SampleTime} ) ) ) );

        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldPassValidationWhenSettingParameterOnEnabledBenchmark()
    {
        // given
        Validation validation = new Validation();
        String benchName = "ComplexOperation";
        boolean isThreadSafe = true;
        Set<String> allowedValues = newHashSet( "1", "3", "10" );
        Set<String> baseValues = newHashSet( "3" );
        String description = "description";
        boolean isEnabled = true;
        BenchmarkDescription benchDesc = new BenchmarkDescription(
                benchName,
                group,
                isThreadSafe,
                METHODS,
                map( "numProblems", new BenchmarkParamDescription( "numProblems", allowedValues, baseValues ) ),
                description,
                isEnabled );

        // when
        benchDesc = benchDesc.copyWithConfig(
                new BenchmarkConfigFileEntry(
                        benchName,
                        true,
                        map( "numProblems", newHashSet( "1" ) ) ),
                validation );

        // then
        assertThat( benchDesc.className(), equalTo( benchName ) );
        assertThat( benchDesc.isThreadSafe(), equalTo( isThreadSafe ) );
        assertThat( benchDesc.description(), equalTo( description ) );
        assertThat( benchDesc.group(), equalTo( group ) );
        assertThat( benchDesc.parameters().size(), equalTo( 1 ) );
        assertThat( benchDesc.parameters().get( "numProblems" ).allowedValues(), equalTo( allowedValues ) );
        assertThat( benchDesc.parameters().get( "numProblems" ).values(), equalTo( newHashSet( "1" ) ) );
        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldIgnoreEmptyApplyConfig()
    {
        // given
        Validation validation = new Validation();
        String benchName = "ComplexOperation";
        boolean isThreadSafe = true;
        Set<String> allowedValues = newHashSet( "1", "3", "10" );
        Set<String> baseValues = newHashSet( "3" );
        String description = "description";
        boolean isEnabled = true;
        BenchmarkDescription benchDesc = new BenchmarkDescription(
                benchName,
                group,
                isThreadSafe,
                METHODS,
                map( "numProblems", new BenchmarkParamDescription( "numProblems", allowedValues, baseValues ) ),
                description,
                isEnabled );

        // when
        benchDesc = benchDesc.copyWithConfig(
                new BenchmarkConfigFileEntry(
                        benchName,
                        false,
                        emptyMap() ),
                validation );

        // then
        assertThat( benchDesc.className(), equalTo( benchName ) );
        assertThat( benchDesc.isThreadSafe(), equalTo( isThreadSafe ) );
        assertThat( benchDesc.description(), equalTo( description ) );
        assertThat( benchDesc.group(), equalTo( group ) );
        assertThat( benchDesc.parameters().size(), equalTo( 1 ) );
        assertThat( benchDesc.parameters().get( "numProblems" ).allowedValues(), equalTo( allowedValues ) );
        assertThat( benchDesc.parameters().get( "numProblems" ).values(), equalTo( newHashSet( "3" ) ) );
        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldFailToApplyConfigToWrongBenchmark()
    {
        // given
        Validation validation = new Validation();
        String benchName = "ComplexOperation";
        Set<String> allowedValues = newHashSet( "1", "3", "10" );
        Set<String> baseValues = newHashSet( "3" );
        String description = "description";
        boolean isEnabled = true;
        BenchmarkDescription benchDesc = new BenchmarkDescription(
                benchName,
                group,
                true,
                METHODS,
                map( "numProblems", new BenchmarkParamDescription( "numProblems", allowedValues, baseValues ) ),
                description,
                isEnabled );

        // when
        String differentBenchName = "DifferentBenchmark";
        BenchmarkConfigFileEntry differentBenchmarkConfigEntry = new BenchmarkConfigFileEntry(
                differentBenchName,
                true,
                map( "numProblems", newHashSet( "1" ) ) );
        try
        {
            benchDesc.copyWithConfig( differentBenchmarkConfigEntry, validation );
            fail( "Expected exception!" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( benchName ) );
            assertThat( e.getMessage(), containsString( differentBenchName ) );
        }
    }

    @Test
    public void shouldFailValidationWhenConfiguredWithUnrecognizedBenchmark()
    {
        // given
        Validation validation = new Validation();
        BenchmarkDescription benchDesc = BenchmarkDescription.of( ValidEnabledBenchmark1.class, validation );

        // when
        String nonExistentBenchmark = "NonExistentBenchmark";
        BenchmarkConfigFileEntry nonExistentBenchmarkConfigEntry = new BenchmarkConfigFileEntry(
                nonExistentBenchmark,
                false,
                map( "numProblems", newHashSet( "1" ) ) );

        SuiteDescription.fromConfig(
                new SuiteDescription( map( benchDesc.className(), benchDesc ) ),
                new BenchmarkConfigFile( map( nonExistentBenchmark, nonExistentBenchmarkConfigEntry ) ),
                validation );

        // then
        assertTrue( validation.report(),
                validation.errorsEqual( CONFIGURED_BENCHMARK_DOES_NOT_EXIST ) );
        assertFalse( validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenConfigValueIsNotAllowed()
    {
        // given
        Validation validation = new Validation();
        String benchName = "ComplexOperation";
        Set<String> allowedValues = newHashSet( "1", "3", "10" );
        Set<String> baseValues = newHashSet( "3" );
        String description = "description";
        boolean isEnabled = true;
        BenchmarkDescription benchDesc = new BenchmarkDescription(
                benchName,
                group,
                true,
                METHODS,
                map( "numProblems", new BenchmarkParamDescription( "numProblems", allowedValues, baseValues ) ),
                description,
                isEnabled );

        // when
        BenchmarkConfigFileEntry config = new BenchmarkConfigFileEntry(
                benchName,
                true,
                map( "numProblems", newHashSet( "-1" ) ) );
        benchDesc.copyWithConfig( config, validation );

        // then
        assertTrue( validation.report(),
                validation.errorsEqual( CONFIGURED_VALUE_IS_NOT_ALLOWED ) );
        assertFalse( validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenConfigParameterDoesNotExist()
    {
        // given
        Validation validation = new Validation();
        String benchName = "ComplexOperation";
        Set<String> allowedValues = newHashSet( "1", "3", "10" );
        Set<String> baseValues = newHashSet( "3" );
        String description = "description";
        boolean isEnabled = true;
        BenchmarkDescription benchDesc = new BenchmarkDescription(
                benchName,
                group,
                true,
                METHODS,
                map( "numProblems", new BenchmarkParamDescription( "numProblems", allowedValues, baseValues ) ),
                description,
                isEnabled );

        // when
        BenchmarkConfigFileEntry config = new BenchmarkConfigFileEntry(
                benchName,
                true,
                map( "doesNotExist", newHashSet( "1" ) ) );
        benchDesc.copyWithConfig( config, validation );

        // then
        assertTrue( validation.report(),
                validation.errorsEqual( CONFIGURED_PARAMETER_DOES_NOT_EXIST ) );
        assertFalse( validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenDuplicateAllowedValues()
    {
        // when
        Validation validation = new Validation();
        BenchmarkDescription.of( DuplicateAllowedBenchmark.class, validation );

        // then
        assertTrue( validation.report(),
                validation.errorsEqual( DUPLICATE_ALLOWED_VALUE ) );
        assertFalse( validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenDuplicateBaseValues()
    {
        // when
        Validation validation = new Validation();
        BenchmarkDescription.of( DuplicateBaseBenchmark.class, validation );

        // then
        assertTrue( validation.report(),
                validation.errorsEqual( DUPLICATE_BASE_VALUE ) );
        assertFalse( validation.isValid() );
    }

    @Test
    public void shouldComputeNumExecutions()
    {
        Validation validation = new Validation();

        BenchmarkDescription benchDesc = BenchmarkDescription.of( ValidEnabledBenchmark1.class, validation );
        assertThat( benchDesc.executionCount( 1 ), equalTo( 6 ) );

        benchDesc = benchDesc.copyWithConfig(
                new BenchmarkConfigFileEntry(
                        benchDesc.className(),
                        true,
                        map( "number", newHashSet( "1", "2" ) ) ),
                validation );
        assertThat( benchDesc.executionCount( 1 ), equalTo( 12 ) );

        benchDesc = benchDesc
                .copyWithConfig(
                        new BenchmarkConfigFileEntry(
                                benchDesc.className(),
                                true,
                                map( "number", newHashSet( "1" ) ) ),
                        validation )
                .copyWithConfig(
                        new BenchmarkConfigFileEntry(
                                benchDesc.className(),
                                true,
                                map( "string", newHashSet( "a" ) ) ),
                        validation );
        assertThat( benchDesc.executionCount( 1 ), equalTo( 3 ) );

        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldExplodeBenchmarkDescription()
    {
        String className = "classname";
        String group = "group";
        boolean isThreadSafe = true;
        String description = "description";
        boolean isEnabled = true;

        String method1Name = "method1";
        Mode[] method1Modes = {Mode.Throughput};
        BenchmarkMethodDescription method1 = new BenchmarkMethodDescription( method1Name, method1Modes );
        String method2Name = "method2";
        Mode[] method2Modes = {Mode.SampleTime};
        BenchmarkMethodDescription method2 = new BenchmarkMethodDescription( method2Name, method2Modes );
        HashMap<String,BenchmarkMethodDescription> methods = new HashMap<>();
        methods.put( method1.name(), method1 );
        methods.put( method2.name(), method2 );

        BenchmarkParamDescription number = new BenchmarkParamDescription(
                "number",
                newHashSet( "1", "2" ),
                newHashSet( "1", "2" ) );
        BenchmarkParamDescription character = new BenchmarkParamDescription(
                "char",
                newHashSet( "a", "b" ),
                newHashSet( "a", "b" ) );
        HashMap<String,BenchmarkParamDescription> parameters = new HashMap<>();
        parameters.put( number.name(), number );
        parameters.put( character.name(), character );

        BenchmarkDescription original = new BenchmarkDescription(
                className,
                group,
                isThreadSafe,
                methods,
                parameters,
                description,
                isEnabled );

            /*
        Unexploded
            {
                [method1, method2],
                [{number,[1,2]},{char,[a,b]}]
            }

        Expected Exploded
            [
                {
                    [method1],
                    [{number,[1]},{char,[a]}]
                },
                {
                    [method1],
                    [{number,[1]},{char,[b]}]
                },
                {
                    [method1],
                    [{number,[2]},{char,[a]}]
                },
                {
                    [method1],
                    [{number,[2]},{char,[b]}]
                },
                {
                    [method2],
                    [{number,[1]},{char,[a]}]
                },
                {
                    [method2],
                    [{number,[1]},{char,[a]}]
                },
                {
                    [method2],
                    [{number,[2]},{char,[b]}]
                },
                {
                    [method2],
                    [{number,[2]},{char,[b]}]
                },
            ]
         */

        BenchmarkParamDescription number1 = new BenchmarkParamDescription(
                "number",
                newHashSet( "1", "2" ),
                newHashSet( "1" ) );
        BenchmarkParamDescription number2 = new BenchmarkParamDescription(
                "number",
                newHashSet( "1", "2" ),
                newHashSet( "2" ) );
        BenchmarkParamDescription characterA = new BenchmarkParamDescription(
                "char",
                newHashSet( "a", "b" ),
                newHashSet( "a" ) );
        BenchmarkParamDescription characterB = new BenchmarkParamDescription(
                "char",
                newHashSet( "a", "b" ),
                newHashSet( "b" ) );
        Set<BenchmarkDescription> expectedExploded = newHashSet(
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method1 ),
                        parametersMap( number1, characterA ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method1 ),
                        parametersMap( number1, characterB ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method1 ),
                        parametersMap( number2, characterA ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method1 ),
                        parametersMap( number2, characterB ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method2 ),
                        parametersMap( number1, characterA ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method2 ),
                        parametersMap( number1, characterB ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method2 ),
                        parametersMap( number2, characterA ),
                        description,
                        isEnabled ),
                new BenchmarkDescription(
                        className,
                        group,
                        isThreadSafe,
                        methodsMap( method2 ),
                        parametersMap( number2, characterB ),
                        description,
                        isEnabled ) );

        Set<BenchmarkDescription> originalExploded = original.explode();

        assertThat( originalExploded, equalTo( expectedExploded ) );
    }

    private static Map<String,BenchmarkMethodDescription> methodsMap( BenchmarkMethodDescription... methods )
    {
        return Stream.of( methods ).collect( toMap( BenchmarkMethodDescription::name, m -> m ) );
    }

    private static Map<String,BenchmarkParamDescription> parametersMap( BenchmarkParamDescription... parameters )
    {
        return Stream.of( parameters ).collect( toMap( BenchmarkParamDescription::name, p -> p ) );
    }

    @Test
    public void shouldExplodeBenchmarkDescriptionParameters()
    {
        BenchmarkParamDescription tom = new BenchmarkParamDescription(
                "tom",
                newHashSet( "a", "b" ),
                newHashSet( "a", "b" ) );
        BenchmarkParamDescription dick = new BenchmarkParamDescription(
                "dick",
                newHashSet( "1", "2" ),
                newHashSet( "1", "2" ) );
        BenchmarkParamDescription harry = new BenchmarkParamDescription(
                "harry",
                newHashSet( "x", "y" ),
                newHashSet( "x", "y" ) );
        ArrayList<BenchmarkParamDescription> parameters = newArrayList( tom, dick, harry );
        Set<Set<BenchmarkParamDescription>> explodedParameters = BenchmarkDescription.explodeParameters( parameters );

        Set<Set<BenchmarkParamDescription>> expectedExplodedParameters = newHashSet(
                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "a" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "1" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "x" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "a" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "1" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "y" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "a" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "2" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "x" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "a" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "2" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "y" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "b" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "1" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "x" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "b" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "1" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "y" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "b" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "2" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "x" ) ) ),

                newHashSet(
                        new BenchmarkParamDescription(
                                "tom",
                                newHashSet( "a", "b" ),
                                newHashSet( "b" ) ),
                        new BenchmarkParamDescription(
                                "dick",
                                newHashSet( "1", "2" ),
                                newHashSet( "2" ) ),
                        new BenchmarkParamDescription(
                                "harry",
                                newHashSet( "x", "y" ),
                                newHashSet( "y" ) ) ) );

        /*
        Unexploded
            [{tom,[a,b]},{dick,[1,2]},{harry,[x,y]}]

        Expected Exploded
            [
                [{tom,[a]},{dick,[1]},{harry,[x]}]
                [{tom,[a]},{dick,[1]},{harry,[y]}]
                [{tom,[a]},{dick,[2]},{harry,[x]}]
                [{tom,[a]},{dick,[2]},{harry,[y]}]
                [{tom,[b]},{dick,[1]},{harry,[x]}]
                [{tom,[b]},{dick,[1]},{harry,[y]}]
                [{tom,[b]},{dick,[2]},{harry,[x]}]
                [{tom,[b]},{dick,[2]},{harry,[y]}]
            ]
         */
        assertThat( explodedParameters.size(), equalTo( expectedExplodedParameters.size() ) );
        assertThat( explodedParameters, equalTo( expectedExplodedParameters ) );
    }

    @Test
    public void shouldExplodeBenchmarkParamDescription()
    {
        BenchmarkParamDescription original = new BenchmarkParamDescription(
                "bob",
                newHashSet( "a", "b", "c" ),
                newHashSet( "a", "b", "c" ) );

        Set<BenchmarkParamDescription> expectedExploded = newHashSet(
                new BenchmarkParamDescription(
                        "bob",
                        newHashSet( "a", "b", "c" ),
                        newHashSet( "a" ) ),
                new BenchmarkParamDescription(
                        "bob",
                        newHashSet( "a", "b", "c" ),
                        newHashSet( "b" ) ),
                new BenchmarkParamDescription(
                        "bob",
                        newHashSet( "a", "b", "c" ),
                        newHashSet( "c" ) ) );

        Set<BenchmarkParamDescription> actualExploded = original.explode();

        assertThat( actualExploded, equalTo( expectedExploded ) );
    }

    @Test
    public void shouldExplodeBenchmarkMethodDescription()
    {
        String methodName = "name";
        Mode[] methodModes = {SampleTime, Throughput};
        BenchmarkMethodDescription original = new BenchmarkMethodDescription( methodName, methodModes );

        Set<BenchmarkMethodDescription> expectedExploded = newHashSet(
                new BenchmarkMethodDescription( methodName, new Mode[]{SampleTime} ),
                new BenchmarkMethodDescription( methodName, new Mode[]{Throughput} ) );

        Set<BenchmarkMethodDescription> actualExploded = original.explode();

        assertThat( actualExploded, equalTo( expectedExploded ) );
    }

    @Test
    public void shouldExplodeEqualNumberOfElementsToExecutionCount()
    {
        Validation validation = new Validation();
        for ( Class benchmarkClass : Annotations.benchmarkClasses() )
        {
            BenchmarkDescription benchmark = BenchmarkDescription.of( benchmarkClass, validation );
            int executionCount = benchmark.executionCount( 1 );
            int explodedSize = benchmark.explode().size();
            assertThat( executionCount, equalTo( explodedSize ) );
        }
        assertThat( validation.report(), validation.isValid() );
    }
}

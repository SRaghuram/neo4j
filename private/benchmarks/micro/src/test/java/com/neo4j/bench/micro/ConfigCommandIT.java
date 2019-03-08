/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.micro.benchmarks.core.ReadById;
import com.neo4j.bench.micro.benchmarks.test_only.ValidDisabledBenchmark;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark1;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark2;
import com.neo4j.bench.micro.config.BenchmarkConfigFile;
import com.neo4j.bench.micro.config.BenchmarkDescription;
import com.neo4j.bench.micro.config.SuiteDescription;
import com.neo4j.bench.micro.config.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.client.util.TestDirectorySupport.createTempFile;
import static com.neo4j.bench.micro.TestUtils.map;
import static com.neo4j.bench.micro.config.BenchmarkConfigFile.fromFile;
import static com.neo4j.bench.micro.config.SuiteDescription.fromConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ExtendWith( TestDirectoryExtension.class )
public class ConfigCommandIT
{
    @Inject
    public TestDirectory temporaryFolder;

    private SuiteDescription suiteDescription;

    @BeforeEach
    public void setup()
    {
        Validation validation = new Validation();
        suiteDescription = SuiteDescription.byReflection( validation );
        assertTrue( validation.isValid(), validation.report() );
    }

    @Test
    // Only benchmarks that are enabled by default should be enabled in the configuration file
    public void shouldWriteDefaultConfig() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        Main.main( new String[]{
                "config", "default",
                "--path", benchmarkConfig.getAbsolutePath()
        } );

        int benchmarkCount = enabledBenchmarkCount( "" );
        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );
        assertThat( configFile.entries().size(), equalTo( benchmarkCount ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( benchmarkCount ) );
    }

    @Test
    public void shouldFailToWriteNonExistentGroupConfig() throws Exception
    {
        // when
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        try
        {
            Main.main( new String[]{
                    "config", "groups",
                    "--path", benchmarkConfig.getAbsolutePath(),
                    "Does Not Exist"
            } );
            fail( "Expected exception!" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( "Unrecognized benchmark group" ) );
        }
    }

    @Test
    public void shouldFailToWriteGroupConfigWhenNoGroupSpecified() throws Exception
    {
        // when
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        try
        {
            Main.main( new String[]{
                    "config", "groups",
                    "--path", benchmarkConfig.getAbsolutePath()
            } );
            fail( "Expected exception!" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( "Expected at least one group, none specified" ) );
        }
    }

    @Test
    public void shouldWriteCoreAPIConfig() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        Main.main( new String[]{
                "config", "groups",
                "--path", benchmarkConfig.getAbsolutePath(),
                "Core API"
        } );

        int benchmarkCount = enabledBenchmarkCount( packagePrefixOf( ReadById.class ) );
        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );
        assertThat( configFile.entries().size(), equalTo( benchmarkCount ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( benchmarkCount ) );
    }

    @Test
    public void shouldWriteGroupConfig() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        Main.main( new String[]{
                "config", "groups",
                "--path", benchmarkConfig.getAbsolutePath(),
                "Example"
        } );

        int benchmarkCount = enabledBenchmarkCount( packagePrefixOf( ValidEnabledBenchmark1.class ) );
        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );
        assertThat( configFile.entries().size(), equalTo( benchmarkCount ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( benchmarkCount ) );
    }

    @Test
    public void shouldFailToWriteNonExistentBenchmarkConfig() throws Exception
    {
        // when
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        try
        {
            Main.main( new String[]{
                    "config", "benchmarks",
                    "--path", benchmarkConfig.getAbsolutePath(),
                    "Does Not Exist"
            } );
            fail( "Expected exception!" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( "Unrecognized benchmark" ) );
        }
    }

    @Test
    public void shouldFailToWriteBenchmarkConfigWhenNoBenchmarkSpecified() throws Exception
    {
        // when
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        try
        {
            Main.main( new String[]{
                    "config", "benchmarks",
                    "--path", benchmarkConfig.getAbsolutePath()
            } );
            fail( "Expected exception!" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( "Expected at least one benchmark, none specified" ) );
        }
    }

    @Test
    public void shouldWriteOnlyEnabledBenchmark() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );

        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertTrue( configFile.hasEntry( benchmarkName ) );
        assertThat( configFile.getEntry( benchmarkName ).name(), equalTo( benchmarkName ) );
        assertThat( configFile.getEntry( benchmarkName ).values(), equalTo( emptyMap() ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( 1 ) );
        assertTrue( finalSuiteDescription.isBenchmark( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).className(), equalTo( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).parameters(),
                equalTo( suiteDescription.getBenchmark( benchmarkName ).parameters() ) );
        assertTrue( finalSuiteDescription.getBenchmark( benchmarkName ).isEnabled() );
    }

    @Test
    public void shouldEnableBenchmarkThatIsDisabledByDefault() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        String benchmarkName = ValidDisabledBenchmark.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );

        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertThat( configFile.getEntry( benchmarkName ).name(), equalTo( benchmarkName ) );
        assertThat( configFile.getEntry( benchmarkName ).values(), equalTo( emptyMap() ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( 1 ) );
        assertTrue( finalSuiteDescription.isBenchmark( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).className(), equalTo( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).parameters(),
                equalTo( suiteDescription.getBenchmark( benchmarkName ).parameters() ) );
        assertTrue( finalSuiteDescription.getBenchmark( benchmarkName ).isEnabled() );
    }

    @Test
    public void shouldWriteOnlyEnabledBenchmarkWhenVerbose() throws Exception
    {
        // when
        Validation validation = new Validation();
        String benchmarkName1 = ValidEnabledBenchmark1.class.getName();
        String benchmarkName2 = ValidEnabledBenchmark2.class.getName();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        Main.main( new String[]{
                "config", "benchmarks",
                "--verbose",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName1,
                benchmarkName2
        } );

        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );

        assertThat( configFile.entries().size(), equalTo( 2 ) );
        assertTrue( configFile.hasEntry( benchmarkName1 ) );
        assertTrue( configFile.getEntry( benchmarkName1 ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName1 ).values().size(), equalTo( 2 ) );
        assertThat( configFile.getEntry( benchmarkName1 ).values(),
                equalTo( map( "number", newHashSet( "1" ),
                        "string", newHashSet( "a", "b" ) ) ) );
        assertTrue( configFile.hasEntry( benchmarkName2 ) );
        assertTrue( configFile.getEntry( benchmarkName2 ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName2 ).values().size(), equalTo( 1 ) );
        assertThat( configFile.getEntry( benchmarkName2 ).values(),
                equalTo( map( "boolean", newHashSet( "true" ) ) ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( 2 ) );

        assertTrue( finalSuiteDescription.isBenchmark( benchmarkName1 ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName1 ).className(),
                equalTo( benchmarkName1 ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName1 ).parameters(),
                equalTo( suiteDescription.getBenchmark( benchmarkName1 ).parameters() ) );
        assertTrue( finalSuiteDescription.getBenchmark( benchmarkName1 ).isEnabled() );

        assertTrue( finalSuiteDescription.isBenchmark( benchmarkName2 ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName2 ).className(),
                equalTo( benchmarkName2 ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName2 ).parameters(),
                equalTo( suiteDescription.getBenchmark( benchmarkName2 ).parameters() ) );
        assertTrue( finalSuiteDescription.getBenchmark( benchmarkName2 ).isEnabled() );
    }

    @Test
    public void shouldWriteAllBenchmarksWhenWithDisabled() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--with-disabled",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );

        assertTrue( validation.isValid(), validation.report() );

        Map<String,Boolean> allBenchmarks = suiteDescription.benchmarks().stream()
                .collect( toMap( BenchmarkDescription::className, benchDesc -> false ) );
        allBenchmarks.put( benchmarkName, true );

        assertThat( configFile.entries().size(), equalTo( allBenchmarks.size() ) );
        // all entries have no parameters set
        assertTrue( configFile.entries().stream()
                .allMatch( entry -> entry.values().isEmpty() ) );
        assertTrue( configFile.entries().stream()
                .allMatch( entry -> entry.isEnabled() == allBenchmarks.get( entry.name() ) ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( 1 ) );
        assertTrue( finalSuiteDescription.isBenchmark( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).className(), equalTo( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).parameters(),
                equalTo( suiteDescription.getBenchmark( benchmarkName ).parameters() ) );
        assertTrue( finalSuiteDescription.getBenchmark( benchmarkName ).isEnabled() );
    }

    @Test
    public void shouldWriteEverythingWhenWithDisabledAndVerbose() throws Exception
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = createTempFile( temporaryFolder.absolutePath() );
        String benchmarkName = ValidDisabledBenchmark.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--with-disabled", "--verbose",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = fromFile( benchmarkConfig.toPath(), validation );

        assertTrue( validation.isValid(), validation.report() );

        Map<String,Boolean> allBenchmarks = suiteDescription.benchmarks().stream()
                .collect( toMap( BenchmarkDescription::className, benchDesc -> false ) );
        allBenchmarks.put( benchmarkName, true );

        assertThat( configFile.entries().size(), equalTo( allBenchmarks.size() ) );
        // all entries for benchmarks that have parameters do have their parameters set
        assertTrue( configFile.entries().stream()
                .allMatch( entry -> entry.values().size() ==
                                    suiteDescription.getBenchmark( entry.name() ).parameters().size() ) );
        assertTrue( configFile.entries().stream()
                .allMatch( entry -> entry.isEnabled() == allBenchmarks.get( entry.name() ) ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( 1 ) );
        assertTrue( finalSuiteDescription.isBenchmark( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).className(), equalTo( benchmarkName ) );
        assertThat( finalSuiteDescription.getBenchmark( benchmarkName ).parameters(),
                equalTo( suiteDescription.getBenchmark( benchmarkName ).parameters() ) );
        assertTrue( finalSuiteDescription.getBenchmark( benchmarkName ).isEnabled() );
    }

    private int enabledBenchmarkCount( String benchmarkNamePrefix )
    {
        return (int) benchmarksWithPrefix( benchmarkNamePrefix ).filter( BenchmarkDescription::isEnabled ).count();
    }

    private Stream<BenchmarkDescription> benchmarksWithPrefix( String benchmarkNamePrefix )
    {
        return suiteDescription.benchmarks().stream()
                .filter( BenchmarkDescription::isEnabled )
                .filter( benchDesc -> benchDesc.className().startsWith( benchmarkNamePrefix ) );
    }

    private String packagePrefixOf( Class<?> clazz )
    {
        String testBenchmark = clazz.getName();
        return testBenchmark.substring( 0, testBenchmark.lastIndexOf( "." ) );
    }
}

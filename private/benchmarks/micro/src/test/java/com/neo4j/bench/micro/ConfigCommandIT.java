/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.micro.benchmarks.core.ReadById;
import com.neo4j.bench.micro.benchmarks.test.ConstantDataConstantAugment;
import com.neo4j.bench.micro.benchmarks.test.DefaultDisabled;
import com.neo4j.bench.micro.benchmarks.test.NoOpBenchmark;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.jmh.api.config.SuiteDescription.fromConfig;
import static com.neo4j.bench.micro.TestUtils.map;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestDirectoryExtension
public class ConfigCommandIT extends AnnotationsFixture
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    // Only benchmarks that are enabled by default should be enabled in the configuration file
    public void shouldWriteDefaultConfig()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        Main.main( new String[]{
                "config", "default",
                "--path", benchmarkConfig.getAbsolutePath()
        } );

        int benchmarkCount = enabledBenchmarkCount( "" );
        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );
        assertThat( configFile.entries().size(), equalTo( benchmarkCount ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                                                                            .filter( BenchmarkDescription::isEnabled )
                                                                            .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( benchmarkCount ) );
    }

    @Test
    public void shouldFailToWriteNonExistentGroupConfig()
    {
        // when
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
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
    public void shouldFailToWriteGroupConfigWhenNoGroupSpecified()
    {
        // when
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
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
    public void shouldWriteCoreAPIConfig()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        Main.main( new String[]{
                "config", "groups",
                "--path", benchmarkConfig.getAbsolutePath(),
                "Core API"
        } );

        int benchmarkCount = enabledBenchmarkCount( ReadById.class.getPackage().getName() );
        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );
        assertThat( configFile.entries().size(), equalTo( benchmarkCount ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                                                                            .filter( BenchmarkDescription::isEnabled )
                                                                            .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( benchmarkCount ) );
    }

    @Test
    public void shouldWriteCypherConfig()
    {
        // with
        Validation validation = new Validation();

        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( "com.neo4j.bench.micro" );
        SuiteDescription suite = SuiteDescription.fromAnnotations( benchmarksFinder, validation );

        List<String> groups = new ArrayList<>( suite.getGroupBenchmarkNames().keySet() );
        groups = groups.stream().filter( s -> !s.equals( "Marketing" ) ).collect( Collectors.toList() );
        System.out.println(groups);
        for ( String group : groups )
        {
            File benchmarkConfig = temporaryFolder.file( "benchmark.config" );
            Main.main( new String[]{
                    "config", "groups",
                    "--path", benchmarkConfig.getAbsolutePath(),
                    group
            } );
            BenchmarkConfigFile benchmarkConfigFileNotPartition = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );
            assertTrue( validation.isValid(), validation.report() );
            SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
            SuiteDescription suiteDescriptionNotPartition = fromConfig( suiteDescription, benchmarkConfigFileNotPartition, validation );
            assertTrue( validation.isValid(), validation.report() );
            Collection<BenchmarkDescription> benchmarkDescriptionsNotPartition = suiteDescriptionNotPartition.explodeEnabledBenchmarks();
            int size = Math.min( benchmarkDescriptionsNotPartition.size(), 16 );

            for ( int partitionSize = 2; partitionSize < size; partitionSize *= 2 )
            {

                File benchmarkConfigFolder = temporaryFolder.directory( group + partitionSize );

                //when
                Main.main( new String[]{
                        "config", "partition",
                        "-p", String.valueOf( partitionSize ),
                        "--config-path", benchmarkConfig.getAbsolutePath(),
                        "-d", benchmarkConfigFolder.getAbsolutePath()
                } );

                File[] configFiles = benchmarkConfigFolder.listFiles();

                List<BenchmarkConfigFile> benchmarkConfigFiles =
                        Arrays.stream( configFiles )
                              .map( partitionedFile -> BenchmarkConfigFile.fromFile( partitionedFile.toPath(), validation, getAnnotations() ) )
                              .collect( toList() );

                assertTrue( validation.isValid(), validation.report() );

                List<SuiteDescription> partitionedSuiteDescription =
                        benchmarkConfigFiles.stream()
                                            .map( benchmarkConfigFile -> fromConfig( suiteDescription, benchmarkConfigFile, validation ) )
                                            .collect( toList() );

                List<Collection<BenchmarkDescription>> benchmarkDescriptions = partitionedSuiteDescription.stream()
                                                                                                          .map( SuiteDescription::explodeEnabledBenchmarks )
                                                                                                          .collect( toList() );

                assertTrue( validation.isValid(), validation.report() );

                //We need to test all benchmarks exists in both.
                assertThat( benchmarkDescriptions.stream().flatMap( Collection::stream ).collect( Collectors.toSet() ),
                            equalTo( new HashSet<>( benchmarkDescriptionsNotPartition ) ) );
                //We need test that we do not have to many duplicates
                List<Integer> sizeOfPartitions = benchmarkDescriptions.stream().map( Collection::size ).collect( toList() );
                double min = Collections.min( sizeOfPartitions );
                double max = Collections.max( sizeOfPartitions );
                double mean = sizeOfPartitions.stream().mapToDouble( val -> val ).average().orElse( 0.0 );
                double distanceFromMean = Math.max( max - mean, mean - min );
                double percentWrong = distanceFromMean / mean;
                Integer partitionedBenchmarkCount = sizeOfPartitions.stream().reduce( 0, Integer::sum );
                //if we should always have at least more benchmarks
                assertThat( partitionedBenchmarkCount, greaterThanOrEqualTo( benchmarkDescriptionsNotPartition.size() ) );
                if ( mean > 20 )
                {
                    assertThat( percentWrong, lessThan( 1.3 ) );
                }
                else if ( mean > 15 || max >= 40 )
                {
                    assertThat( distanceFromMean, lessThan( 26.0 ) );
                }
                else if ( mean > 9 )
                {
                    assertThat( distanceFromMean, lessThan( 20.0 ) );
                }
                else
                {
                    assertThat( distanceFromMean, lessThanOrEqualTo( 7.0 ) );
                }

                //We need to test that benchmarks exists in a few partitions
                HashMap<BenchmarkDescription,Integer> benchmarkDescriptionCountMap = new HashMap<>();
                benchmarkDescriptions.stream()
                                     .flatMap( Collection::stream )
                                     .forEach( bd ->
                                               {
                                                   Integer sum =
                                                           benchmarkDescriptionCountMap.getOrDefault( bd, 0 ) + 1;
                                                   benchmarkDescriptionCountMap.put( bd, sum );
                                               } );
                assertThat( Collections.max( benchmarkDescriptionCountMap.values() ), lessThanOrEqualTo( 24 ) );
            }
        }
    }

    @Test
    public void shouldWriteGroupConfig()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        Main.main( new String[]{
                "config", "groups",
                "--path", benchmarkConfig.getAbsolutePath(),
                "TestOnly"
        } );

        int benchmarkCount = enabledBenchmarkCount( NoOpBenchmark.class.getPackage().getName() );
        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );
        assertThat( configFile.entries().size(), equalTo( benchmarkCount ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        SuiteDescription finalSuiteDescription = fromConfig( suiteDescription, configFile, validation );
        assertTrue( validation.isValid(), validation.report() );

        List<BenchmarkDescription> enabledBenchmarks = finalSuiteDescription.benchmarks().stream()
                                                                            .filter( BenchmarkDescription::isEnabled )
                                                                            .collect( toList() );
        assertThat( enabledBenchmarks.size(), equalTo( benchmarkCount ) );
    }

    @Test
    public void shouldFailToWriteNonExistentBenchmarkConfig()
    {
        // when
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
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
    public void shouldFailToWriteBenchmarkConfigWhenNoBenchmarkSpecified()
    {
        // when
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
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
    public void shouldWriteOnlyEnabledBenchmark()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        String benchmarkName = NoOpBenchmark.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );

        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertTrue( configFile.hasEntry( benchmarkName ) );
        assertThat( configFile.getEntry( benchmarkName ).name(), equalTo( benchmarkName ) );
        assertThat( configFile.getEntry( benchmarkName ).values(), equalTo( emptyMap() ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
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
    public void shouldEnableBenchmarkThatIsDisabledByDefault()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        String benchmarkName = DefaultDisabled.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );

        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertThat( configFile.getEntry( benchmarkName ).name(), equalTo( benchmarkName ) );
        assertThat( configFile.getEntry( benchmarkName ).values(), equalTo( emptyMap() ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
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
    public void shouldWriteOnlyEnabledBenchmarkWhenVerbose()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName1 = ConstantDataConstantAugment.class.getName();
        String benchmarkName2 = NoOpBenchmark.class.getName();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        Main.main( new String[]{
                "config", "benchmarks",
                "--verbose",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName1,
                benchmarkName2
        } );

        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );

        assertThat( configFile.entries().size(), equalTo( 2 ) );
        assertTrue( configFile.hasEntry( benchmarkName1 ) );
        assertTrue( configFile.getEntry( benchmarkName1 ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName1 ).values().size(), equalTo( 1 ) );
        assertThat( configFile.getEntry( benchmarkName1 ).values(),
                    equalTo( map( "extraNodes", newHashSet( "1", "2" ) ) ) );
        assertTrue( configFile.hasEntry( benchmarkName2 ) );
        assertTrue( configFile.getEntry( benchmarkName2 ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName2 ).values().size(), equalTo( 0 ) );
        assertThat( configFile.getEntry( benchmarkName2 ).values(), equalTo( emptyMap() ) );

        assertTrue( validation.isValid(), validation.report() );
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
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
    public void shouldWriteAllBenchmarksWhenWithDisabled()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        String benchmarkName = NoOpBenchmark.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--with-disabled",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );

        assertTrue( validation.isValid(), validation.report() );

        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
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
    public void shouldWriteEverythingWhenWithDisabledAndVerbose()
    {
        // when
        Validation validation = new Validation();
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        String benchmarkName = DefaultDisabled.class.getName();
        Main.main( new String[]{
                "config", "benchmarks",
                "--with-disabled", "--verbose",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmarkName
        } );

        BenchmarkConfigFile configFile = BenchmarkConfigFile.fromFile( benchmarkConfig.toPath(), validation, getAnnotations() );

        assertTrue( validation.isValid(), validation.report() );

        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
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
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        return suiteDescription.benchmarks().stream()
                               .filter( BenchmarkDescription::isEnabled )
                               .filter( benchDesc -> benchDesc.className().startsWith( benchmarkNamePrefix ) );
    }
}

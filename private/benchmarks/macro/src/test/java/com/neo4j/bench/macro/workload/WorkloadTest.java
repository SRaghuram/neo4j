/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Resources;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.util.TestDirectorySupport.createTempDirectoryPath;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class WorkloadTest
{
    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void allWorkloadsShouldHaveUniqueName() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Map<String,List<Workload>> workloadsByName = Workload.allWorkloads( resources, Deployment.embedded() ).stream()
                                                                 .collect( Collectors.groupingBy( Workload::name ) );

            List<String> duplicateWorkloadNames = workloadsByName.keySet().stream()
                                                                 .filter( workloadName -> workloadsByName.get( workloadName ).size() != 1 )
                                                                 .collect( toList() );

            assertTrue( duplicateWorkloadNames.isEmpty(), duplicateWorkloadNames.toString() );
        }
    }

    @Test
    void allWorkloadsShouldHaveConfigurationFile() throws Exception
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadsDir = resources.getResourceFile( "/workloads" );
            try ( Stream<Path> workloadDirs = Files.list( workloadsDir )
                                                   .filter( Files::isDirectory ) )
            {
                workloadDirs.forEach( workloadDir -> assertTrue( containsAtLeastOneConfigurationFile( workloadDir ),
                                                                 "Workload directory did not contain config file" + workloadDir.toAbsolutePath() ) );
            }
        }
    }

    @Test
    void workloadsShouldHaveCorrectName() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() )
                    .forEach( workload ->
                              {
                                  String expectedWorkloadName =
                                          workload.configFile().getFileName().toString().replace( ".json",
                                                                                                  "" );
                                  assertThat( String.format( "Workload with config: %s%n" +
                                                             "Should have name: %s%n" +
                                                             "But had name: %s",
                                                             workload.configFile(),
                                                             expectedWorkloadName,
                                                             workload.name() ),
                                              workload.name(),
                                              equalTo( expectedWorkloadName ) );
                              } );
        }
    }

    @Test
    void workloadsShouldHaveAtLeastOneQuery() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() ).forEach( workload -> assertFalse( workload.queries().isEmpty() ) );
        }
    }

    @Test
    void workloadQueriesShouldHaveAllFieldsPopulated() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() ).stream()
                    .flatMap( workload -> workload.queries().stream() )
                    .forEach( query ->
                              {
                                  // Very naive asserts, just checking that every query has been populated with non-null values
                                  assertThat( query.benchmarkGroup(), not( is( nullValue() ) ) );
                                  assertThat( query.name(), not( is( nullValue() ) ) );
                                  assertThat( query.description(), not( is( nullValue() ) ) );
                                  assertThat( query.warmupQueryString(), not( is( nullValue() ) ) );
                                  assertThat( query.queryString(), not( is( nullValue() ) ) );
                                  assertThat( query.isSingleShot(), not( is( nullValue() ) ) );
                                  assertThat( query.parameters(), not( is( nullValue() ) ) );
                              } );
        }
    }

    // This test can be removed once procedure support is added
    @Test
    void queriesShouldNotCallProcedures() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() ).stream()
                    .flatMap( workload -> workload.queries().stream() )
                    .forEach( query ->
                              {
                                  assertFalse( query.queryString().value().contains( "CALL " ) );
                                  assertFalse( query.warmupQueryString().value().contains( "CALL " ) );
                              } );
        }
    }

    @Test
    void shouldParseAllParameterFiles() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() )
                    .forEach( workload ->
                              {
                                  for ( Query query : workload.queries() )
                                  {
                                      Parameters parameters = query.parameters();
                                      try ( ParametersReader parametersReader = parameters.create() )
                                      {
                                          for ( int i = 0; i < 10_000 && parametersReader.hasNext(); i++ )
                                          {
                                              assertThat( parametersReader.next(),
                                                          not( is( nullValue() ) ) );
                                          }
                                      }
                                      catch ( Exception e )
                                      {
                                          throw new RuntimeException( "Could not parse parameters\n" +
                                                                      "Workload : " +
                                                                      workload.configFile().toAbsolutePath() +
                                                                      "\n" +
                                                                      "Query    : " + query.name(), e );
                                      }
                                  }
                              } );
        }
    }

    @Test
    void shouldParseValidWorkload() throws Exception
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadDir = resources.getResourceFile( "/test_workloads/test" );
            Path validWorkloadConfig = workloadDir.resolve( "valid.json" );
            Workload workload = Workload.fromFile( validWorkloadConfig, Deployment.embedded() );
            String expectedWorkloadName = workload.configFile().getFileName().toString().replace( ".json", "" );
            assertThat( String.format( "Workload with config: %s%n" +
                                       "Should have name: %s%n" +
                                       "But had name: %s",
                                       workload.configFile(),
                                       expectedWorkloadName,
                                       workload.name() ),
                        workload.name(),
                        equalTo( expectedWorkloadName ) );
            List<Query> queries = workload.queries();
            assertThat( queries.size(), equalTo( 4 ) );

            Query query1 = queries.get( 0 );
            assertThat( query1.name(), equalTo( "Q1" ) );
            assertThat( query1.description(), equalTo( "D1" ) );
            assertThat( query1.isSingleShot(), equalTo( true ) );
            assertThat( query1.isMutating(), equalTo( true ) );
            assertThat( query1.hasWarmup(), equalTo( Query.DEFAULT_HAS_WARMUP ) );
            assertThat( BenchmarkUtil.lessWhiteSpace( query1.warmupQueryString().value().trim() ), equalTo( "MATCH (n) RETURN n" ) );
            assertThat( BenchmarkUtil.lessWhiteSpace( query1.queryString().value().trim() ), equalTo( "MATCH (n) RETURN n LIMIT 1" ) );

            assertTrue( query1.parameters().isLoopable() );
            ParametersReader parameters1 = query1.parameters().create();
            assertTrue( parameters1.hasNext() );
            Map<String,Object> paramMap1 = parameters1.next();
            assertThat( paramMap1.get( "Param1" ), equalTo( "a" ) );
            assertThat( paramMap1.get( "Param2" ), equalTo( "b" ) );
            assertThat( parameters1.hasNext(), equalTo( query1.parameters().isLoopable() ) );

            Query query2 = queries.get( 1 );
            assertThat( query2.name(), equalTo( "Q2" ) );
            assertThat( query2.description(), equalTo( Query.DEFAULT_DESCRIPTION ) );
            assertThat( query2.isSingleShot(), equalTo( false ) );
            assertThat( query2.isMutating(), equalTo( Query.DEFAULT_IS_MUTATING ) );
            assertThat( query2.hasWarmup(), equalTo( false ) );
            assertThat( query2.warmupQueryString().value(), equalTo( query2.queryString().value() ) );
            assertThat( BenchmarkUtil.lessWhiteSpace( query2.queryString().value().trim() ), equalTo( "MATCH (n) RETURN count(n)" ) );

            assertFalse( query2.parameters().isLoopable() );
            ParametersReader parameters2 = query2.parameters().create();
            assertTrue( parameters2.hasNext() );
            Map<String,Object> paramMap2 = parameters2.next();
            assertThat( paramMap2.get( "Param1" ), equalTo( "a" ) );
            assertThat( paramMap2.get( "Param2" ), equalTo( "b" ) );
            assertThat( parameters2.hasNext(), equalTo( query2.parameters().isLoopable() ) );

            Query query3 = queries.get( 2 );
            assertThat( query3.name(), equalTo( "Q3" ) );
            assertThat( query3.description(), equalTo( Query.DEFAULT_DESCRIPTION ) );
            assertThat( query3.isSingleShot(), equalTo( Query.DEFAULT_IS_SINGLE_SHOT ) );
            assertThat( query3.isMutating(), equalTo( false ) );
            assertThat( query3.hasWarmup(), equalTo( true ) );
            assertThat( query3.warmupQueryString().value(), equalTo( query3.queryString().value() ) );
            assertThat( query3.queryString().value().trim(), equalTo( "MATCH (n) RETURN n.foo" ) );

            assertThat( query3.parameters().isLoopable(), equalTo( Parameters.IS_LOOPABLE_DEFAULT ) );
            ParametersReader parameters3 = query3.parameters().create();
            assertTrue( parameters3.hasNext() );
            Map<String,Object> paramMap3 = parameters3.next();
            assertThat( paramMap3.get( "Param1" ), equalTo( "a" ) );
            assertThat( paramMap3.get( "Param2" ), equalTo( "b" ) );
            assertThat( parameters3.hasNext(), equalTo( query3.parameters().isLoopable() ) );

            Query query4 = queries.get( 3 );
            assertThat( query4.name(), equalTo( "Q4" ) );
            assertThat( query4.description(), equalTo( Query.DEFAULT_DESCRIPTION ) );
            assertThat( query4.isSingleShot(), equalTo( Query.DEFAULT_IS_SINGLE_SHOT ) );
            assertThat( query4.isMutating(), equalTo( Query.DEFAULT_IS_MUTATING ) );
            assertThat( query4.hasWarmup(), equalTo( Query.DEFAULT_HAS_WARMUP ) );
            assertThat( BenchmarkUtil.lessWhiteSpace( query4.warmupQueryString().value().trim() ), equalTo( "MATCH (n) RETURN n ORDER BY n" ) );
            assertThat( query4.queryString().value().trim(), equalTo( "MATCH (n) RETURN n.bar" ) );

            assertThat( query4.parameters().isLoopable(), equalTo( Parameters.IS_LOOPABLE_DEFAULT ) );
            ParametersReader parameters4 = query4.parameters().create();
            int forever = 10;
            for ( int i = 0; i < forever; i++ )
            {
                assertTrue( parameters4.hasNext() );
                assertTrue( parameters4.next().isEmpty() );
            }
        }
    }

    @Test
    void shouldFailToParseWhenEmptyQueries() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_empty_queries.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.EMPTY_QUERIES ) );
        }
    }

    @Test
    void shouldFailToParseWhenMissingParamFile() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_missing_parameters_file.json" );

            for ( Query query : Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ).queries() )
            {
                WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                           () -> query.parameters().create() );
                assertThat( e.error(), equalTo( WorkloadConfigError.PARAM_FILE_NOT_FOUND ) );
            }
        }
    }

    @Test
    void shouldFailToParseWhenMissingQueryFile() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_missing_query_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.QUERY_FILE_NOT_FOUND ) );
        }
    }

    @Test
    void shouldFailToParseWhenMissingSchemaFile() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_missing_schema_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.SCHEMA_FILE_NOT_FOUND ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoParamFile() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_parameters_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_PARAM_FILE ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoQueries() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_queries.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_QUERIES ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoQueryFile() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_query_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_QUERY_FILE ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoQueryName() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_query_name.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_QUERY_NAME ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoSchema() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_schema.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_SCHEMA ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoWorkloadName() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_workload_name.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_WORKLOAD_NAME ) );
        }
    }

    @Test
    void shouldFailToParseWhenInvalidQueryKey() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_query_key.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.INVALID_QUERY_FIELD ) );
        }
    }

    @Test
    void shouldFailToParseWhenInvalidWorkloadKey() throws IOException
    {
        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_workload_key.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.INVALID_WORKLOAD_FIELD ) );
        }
    }

    private boolean containsAtLeastOneConfigurationFile( Path workloadDir ) throws UncheckedIOException
    {
        return Workload.workloadConfigFilesIn( workloadDir ).anyMatch( file -> file.toString().endsWith( ".json" ) );
    }
}

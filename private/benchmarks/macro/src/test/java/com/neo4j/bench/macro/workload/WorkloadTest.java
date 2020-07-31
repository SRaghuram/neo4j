/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import static java.lang.String.format;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class WorkloadTest
{
    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void onlyPeriodicCommitQueriesShouldHaveWarmupQueries()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                for ( Query query : workload.queries() )
                {
                    if ( query.warmupQueryString().isPresent() )
                    {
                        assertFalse( query.warmupQueryString().get().isPeriodicCommit(),
                                     errMsg( "Warmup query not allowed to do PERIODIC COMMIT", workload, query ) );
                        assertTrue( query.queryString().isPeriodicCommit(),
                                    errMsg( "Only PERIODIC COMMIT queries should have warmup query", workload, query ) );
                    }
                }
            }
        }
    }

    @Test
    // NOTE: test is a bit weak because, e.g., a query may contain some mutating clause name in a string, which is totally valid.
    //       that is not the case for any query existing at this time, however, and having this sanity may protect us from quietly doing dumb things in future.
    void shouldAlwaysMarkMutatingQueriesAsMutating()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                for ( Query query : workload.queries() )
                {
                    String cypherString = query.queryString().value();
                    boolean isReallyMutating = cypherString.contains( "SET " ) ||
                                               cypherString.contains( "REMOVE " ) ||
                                               cypherString.contains( "CREATE " ) ||
                                               cypherString.contains( "DELETE " ) ||
                                               cypherString.contains( "MERGE " );
                    assertThat( errMsg( "Mutating queries should be marked as `isMutating`, and vice versa\n" +
                                        "`isMutating`:               " + query.isMutating() + "\n" +
                                        "But query actually mutates: " + isReallyMutating, workload, query ),
                                isReallyMutating,
                                equalTo( query.isMutating() ) );
                }
            }
        }
    }

    @Test
    // NOTE: test is a bit weak because, e.g., a query may contain some mutating clause name in a string, which is totally valid.
    //       that is not the case for any query existing at this time, however, and having this sanity may protect us from quietly doing dumb things in future.
    void shouldNeverHaveMutatingWarmupQueries()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                for ( Query query : workload.queries() )
                {
                    QueryString warmupQuery = query.warmupQueryString().orElse( null );
                    if ( null != warmupQuery )
                    {
                        String cypherString = warmupQuery.value();
                        assertFalse( cypherString.contains( "SET " ),
                                     errMsg( "Warmup query not allowed to mutate but contains 'SET'", workload, query ) );
                        assertFalse( cypherString.contains( "REMOVE " ),
                                     errMsg( "Warmup query not allowed to mutate but contains 'REMOVE'", workload, query ) );
                        assertFalse( cypherString.contains( "CREATE " ),
                                     errMsg( "Warmup query not allowed to mutate but contains 'CREATE'", workload, query ) );
                        assertFalse( cypherString.contains( "DELETE " ),
                                     errMsg( "Warmup query not allowed to mutate but contains 'DELETE'", workload, query ) );
                        assertFalse( cypherString.contains( "MERGE " ),
                                     errMsg( "Warmup query not allowed to mutate but contains 'MERGE'", workload, query ) );
                    }
                }
            }
        }
    }

    private String errMsg( String error, Workload workload, Query query )
    {
        return format( "%s\n" +
                       "Workload: %s\n" +
                       "Query:    %s\n" +
                       "----------------- Warmup Query ----------------------\n" +
                       "%s\n" +
                       "----------------- Measure Query ---------------------\n" +
                       "%s\n" +
                       "-----------------------------------------------------",
                       error,
                       workload.name(),
                       query.name(),
                       query.warmupQueryString().map( QueryString::value ).orElse( "n/a" ),
                       query.queryString().value() );
    }

    @Test
    void allWorkloadsShouldHaveUniqueName()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
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
    void allWorkloadsShouldHaveExactlyOneConfigurationFile() throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadsDir = resources.getResourceFile( "/workloads" );
            try ( Stream<Path> workloadDirs = Files.list( workloadsDir )
                                                   .filter( Files::isDirectory ) )
            {
                workloadDirs.forEach( workloadDir ->
                                      {
                                          List<String> configFilenames = configurationFilesIn( workloadDir );
                                          assertThat( "Workload directory did not contain exactly one config file\n" +
                                                      "Workload folder:     " + workloadDir.toAbsolutePath() + "\n" +
                                                      String.join( "\n", configFilenames ),
                                                      configFilenames.size(),
                                                      equalTo( 1 ) );
                                      } );
            }
        }
    }

    @Test
    void workloadsShouldHaveCorrectName()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() )
                    .forEach( workload ->
                              {
                                  String expectedWorkloadName =
                                          workload.configFile().getFileName().toString().replace( ".json",
                                                                                                  "" );
                                  assertThat( format( "Workload with config: %s%n" +
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
    void workloadsShouldHaveAtLeastOneQuery()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Workload.allWorkloads( resources, Deployment.embedded() ).forEach( workload -> assertFalse( workload.queries().isEmpty() ) );
        }
    }

    @Test
    public void workloadsShouldHaveUniquelyNamedQueries() throws IOException
    {
        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            for ( Workload workload : Workload.allWorkloads( resources, Deployment.embedded() ) )
            {
                List<String> duplicateNames = workload.queries()
                                                      .stream()
                                                      .collect( groupingBy( Query::name, counting() ) )
                                                      .entrySet()
                                                      .stream()
                                                      .filter( e -> e.getValue() > 1 )
                                                      .map( Map.Entry::getKey )
                                                      .collect( toList() );

                assertTrue( duplicateNames.isEmpty(),
                            format( "Workload '%s' contains multiple queries with same name: '%s'", workload.name(), duplicateNames ) );
            }
        }
    }

    @Test
    void workloadQueriesShouldHaveAllFieldsPopulated()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
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

    @Test
    void shouldParseAllParameterFiles()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
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
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadDir = resources.getResourceFile( "/test_workloads/test" );
            Path validWorkloadConfig = workloadDir.resolve( "valid.json" );
            Workload workload = Workload.fromFile( validWorkloadConfig, Deployment.embedded() );
            String expectedWorkloadName = workload.configFile().getFileName().toString().replace( ".json", "" );
            assertEquals( 2, workload.queryPartitionSize() );
            assertThat( format( "Workload with config: %s%n" +
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
            assertThat( query1.warmupQueryString().isPresent(), equalTo( true ) );
            assertThat( BenchmarkUtil.lessWhiteSpace( query1.warmupQueryString().get().value().trim() ), equalTo( "MATCH (n) RETURN n" ) );
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
            assertThat( query2.warmupQueryString().isPresent(), equalTo( false ) );
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
            assertThat( query3.warmupQueryString().isPresent(), equalTo( false ) );
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
            assertThat( query4.warmupQueryString().isPresent(), equalTo( true ) );
            assertThat( BenchmarkUtil.lessWhiteSpace( query4.warmupQueryString().get().value().trim() ), equalTo( "MATCH (n) RETURN n ORDER BY n" ) );
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
    void shouldFailToParseWhenEmptyQueries()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_empty_queries.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.EMPTY_QUERIES ) );
        }
    }

    @Test
    void shouldFailToParseWhenMissingParamFile()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
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
    void shouldFailToParseWhenMissingQueryFile()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_missing_query_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.QUERY_FILE_NOT_FOUND ) );
        }
    }

    @Test
    void shouldFailToParseWhenMissingSchemaFile()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_missing_schema_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.SCHEMA_FILE_NOT_FOUND ) );
        }
    }

    @Test
    void shouldFailToParseWhenSchemaFileContainsDuplicates()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_schema_with_duplicates.json" );

            Workload.fromFile( workloadConfigurationFile, Deployment.embedded() );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.INVALID_SCHEMA_ENTRY ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoParamFile()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_parameters_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_PARAM_FILE ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoQueries()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_queries.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_QUERIES ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoQueryFile()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_query_file.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_QUERY_FILE ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoQueryName()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_query_name.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_QUERY_NAME ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoSchema()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_schema.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_SCHEMA ) );
        }
    }

    @Test
    void shouldFailToParseWhenNoWorkloadName()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_no_workload_name.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.NO_WORKLOAD_NAME ) );
        }
    }

    @Test
    void shouldFailToParseWhenInvalidQueryKey()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_query_key.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.INVALID_QUERY_FIELD ) );
        }
    }

    @Test
    void shouldFailToParseWhenInvalidWorkloadKey()
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path workloadConfigurationFile = resources.getResourceFile( "/test_workloads/test/invalid_workload_key.json" );

            WorkloadConfigException e = BenchmarkUtil.assertException( WorkloadConfigException.class,
                                                                       () -> Workload.fromFile( workloadConfigurationFile, Deployment.embedded() ) );
            assertThat( e.error(), equalTo( WorkloadConfigError.INVALID_WORKLOAD_FIELD ) );
        }
    }

    private List<String> configurationFilesIn( Path workloadDir ) throws UncheckedIOException
    {
        return Workload.workloadConfigFilesIn( workloadDir )
                       .map( Path::toString )
                       .filter( filename -> filename.endsWith( ".json" ) )
                       .collect( toList() );
    }
}

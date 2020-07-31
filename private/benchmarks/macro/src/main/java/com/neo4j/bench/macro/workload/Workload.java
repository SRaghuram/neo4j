/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.neo4j.bench.common.database.DatabaseName;
import com.neo4j.bench.common.database.Neo4jDatabaseNames;
import com.neo4j.bench.common.tool.macro.DeploymentMode;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.model.model.BenchmarkGroup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Workload
{
    private static final String NAME = "name";
    private static final String QUERIES = "queries";
    private static final String SCHEMA = "schema";
    private static final String DATABASE_NAME = "databaseName";
    private static final String QUERY_PARTITION_SIZE = "queryPartitionSize";

    private final List<Query> queries;
    private final String name;
    private final Path configFile;
    private final Schema schema;
    private final DatabaseName databaseName;
    private final int queryPartitionSize;

    public static Workload fromName( String workloadName, Resources resources, DeploymentMode mode )
    {
        return fromFile( workloadFileForName( workloadName, resources, mode ), mode );
    }

    public static List<Workload> all( Resources resources, DeploymentMode mode )
    {
        List<Path> allWorkloadFiles = allWorkloadFiles( resources );
        return allWorkloadFiles.stream()
                               .filter( file -> file.toString().toLowerCase().endsWith( ".json" ) )
                               .map( file -> fromFile( file, mode ) )
                               .collect( toList() );
    }

    private static Path workloadFileForName( String workloadName, Resources resources, DeploymentMode mode )
    {
        List<Path> allWorkloadFiles = allWorkloadFiles( resources );
        return allWorkloadFiles.stream()
                               .filter( file -> file.getFileName().toString().equalsIgnoreCase( workloadName + ".json" ) )
                               .findFirst()
                               .orElseThrow( () -> new RuntimeException( "No workload found for name: " + workloadName + "\n" +
                                                                         "Valid workloads:\n\t" +
                                                                         allWorkloads( resources, mode ).stream()
                                                                                                        .map( Workload::name )
                                                                                                        .collect( joining( "\n\t" ) ) ) );
    }

    static List<Workload> allWorkloads( Resources resources, DeploymentMode mode )
    {
        return allWorkloadFiles( resources ).stream()
                                            .map( path -> Workload.fromFile( path, mode ) )
                                            .collect( toList() );
    }

    private static List<Path> allWorkloadFiles( Resources resources )
    {
        try
        {
            return Files.list( workloadsDir( resources ) )
                        .filter( Files::isDirectory )
                        .flatMap( Workload::workloadConfigFilesIn )
                        .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error listing files", e );
        }
    }

    static Stream<Path> workloadConfigFilesIn( Path workloadDir ) throws UncheckedIOException
    {
        try
        {
            return Files.list( workloadDir )
                        .filter( file -> file.toString().endsWith( ".json" ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static Path workloadsDir( Resources resources )
    {
        return resources.getResourceFile( "/workloads" );
    }

    static Workload fromFile( Path workloadConfigFile, DeploymentMode mode )
    {
        try
        {
            // TODO find out why this does not work
//            Map<String,Object> config = JsonUtil.deserializeJson( workloadConfigFile, Map.class );
            Map<String,Object> config = deserializeGroupConfigJson( workloadConfigFile );
            assertConfigHasAllAndOnlyExpectedKeys( config );
            String workloadName = ((String) config.get( NAME )).trim();
            List<Query> queries = AdditionalQueries.queriesFor( workloadName, mode );

            if ( config.containsKey( QUERIES ) )
            {
                List<Query> staticQueries = getQueries( (List<Map<String,Object>>) config.get( QUERIES ),
                                                        workloadName,
                                                        workloadConfigFile.getParent(),
                                                        mode );
                queries.addAll( staticQueries );
            }
            // if there are no "additional queries", it is compulsory that static queries are configured.
            // no workload queries were statically configured.
            else if ( queries.isEmpty() )
            {
                throw new WorkloadConfigException( WorkloadConfigError.NO_QUERIES );
            }

            // static queries were configured, but the list was empty
            if ( queries.isEmpty() )
            {
                throw new WorkloadConfigException( "No queries found in: " + workloadConfigFile, WorkloadConfigError.EMPTY_QUERIES );
            }
            Path schemaFile = workloadConfigFile.getParent().resolve( (String) config.get( SCHEMA ) );
            Schema schema = loadSchema( schemaFile );
            Optional<List<Schema.SchemaEntry>> maybeDuplicates = schema.duplicates();
            if ( maybeDuplicates.isPresent() )
            {
                final String duplicateSchemaEntries = maybeDuplicates.get().stream()
                                                                     .map( e -> "\t" + e.description() )
                                                                     .collect( joining( "\n" ) );
                throw new WorkloadConfigException( "Duplicate Schema Entries:\n" + duplicateSchemaEntries + "\n", WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
            DatabaseName databaseName = Neo4jDatabaseNames.ofNullable( (String) config.get( DATABASE_NAME ) );

            int queryPartitionSize = 1;
            if ( config.containsKey( QUERY_PARTITION_SIZE ) )
            {
                queryPartitionSize = Integer.parseInt( config.get( QUERY_PARTITION_SIZE ).toString() );
            }

            return new Workload( queries, workloadName, workloadConfigFile, schema, databaseName, queryPartitionSize );
        }
        catch ( WorkloadConfigException e )
        {
            throw new WorkloadConfigException( "Error loading workload from file: " + workloadConfigFile.toAbsolutePath(), e.error(), e );
        }
    }

    private static List<Query> getQueries( List<Map<String,Object>> queryConfigurations,
                                           String workloadName,
                                           Path workloadDir,
                                           DeploymentMode mode )
    {
        return queryConfigurations.stream()
                                  .map( queryConfig -> Query.from( queryConfig, workloadName, workloadDir, mode ) )
                                  .collect( toList() );
    }

    private static Schema loadSchema( Path schemaFile )
    {
        try
        {
            if ( !Files.exists( schemaFile ) )
            {
                throw new WorkloadConfigException( "Schema file not found: " + schemaFile, WorkloadConfigError.SCHEMA_FILE_NOT_FOUND );
            }
            else
            {
                String schemaFileContents = CharStreams.toString( new InputStreamReader( Files.newInputStream( schemaFile ), StandardCharsets.UTF_8 ) );
                List<String> schemaEntries = Arrays.stream( schemaFileContents.split( "\n" ) )
                                                   .filter( line -> !line.trim().isEmpty() )
                                                   .filter( line -> !line.trim().startsWith( "//" ) )
                                                   .map( String::trim )
                                                   .collect( toList() );
                return Schema.loadFrom( schemaEntries );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error loading schema from: " + schemaFile.toAbsolutePath(), e );
        }
    }

    private static void assertConfigHasAllAndOnlyExpectedKeys( Map<String,Object> workloadConfig )
    {
        Set<String> invalidKeys = Sets.newHashSet( workloadConfig.keySet() );
        // not compulsory, and is checked for elsewhere
        invalidKeys.remove( QUERIES );
        invalidKeys.remove( DATABASE_NAME );
        // has default value, is optional
        invalidKeys.remove( QUERY_PARTITION_SIZE );

        if ( !workloadConfig.containsKey( NAME ) )
        {
            throw new WorkloadConfigException( WorkloadConfigError.NO_WORKLOAD_NAME );
        }
        else
        {
            invalidKeys.remove( NAME );
        }

        if ( !workloadConfig.containsKey( SCHEMA ) )
        {
            throw new WorkloadConfigException( WorkloadConfigError.NO_SCHEMA );
        }
        else
        {
            invalidKeys.remove( SCHEMA );
        }

        if ( !invalidKeys.isEmpty() )
        {
            throw new WorkloadConfigException( format( "Workload config contained unrecognized keys: %s", invalidKeys ),
                                               WorkloadConfigError.INVALID_WORKLOAD_FIELD );
        }
    }

    // TODO remove
    private static Map<String,Object> deserializeGroupConfigJson( Path file )
    {
        try ( BufferedReader reader = Files.newBufferedReader( file, UTF_8 ) )
        {
            return new ObjectMapper().readerFor( Map.class ).readValue( reader );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to deserialize: " + file, e );
        }
    }

    private Workload( List<Query> queries, String name, Path configFile, Schema schema, DatabaseName databaseName, int queryPartitionSize )
    {
        this.queries = queries;
        this.name = name;
        this.configFile = configFile;
        this.schema = schema;
        this.databaseName = Objects.requireNonNull( databaseName );
        this.queryPartitionSize = queryPartitionSize;
    }

    public Schema expectedSchema()
    {
        return schema;
    }

    public List<Query> queries()
    {
        return queries;
    }

    public Query queryForName( String queryName )
    {
        return queries.stream()
                      .filter( query -> query.name().equalsIgnoreCase( queryName ) )
                      .findFirst()
                      .orElseThrow( () -> new RuntimeException( String.format( "'%s' is not a valid query name for workload %s\n" +
                                                                               "Valid names are: %s",
                                                                               queryName,
                                                                               name,
                                                                               queries.stream().map( Query::name ).collect( toList() ) ) ) );
    }

    public BenchmarkGroup benchmarkGroup()
    {
        return new BenchmarkGroup( name );
    }

    public String name()
    {
        return name;
    }

    public int queryPartitionSize()
    {
        return queryPartitionSize;
    }

    Path configFile()
    {
        return configFile;
    }

    @Override
    public String toString()
    {
        return "Workload\n" +
               "\tname        : " + name + "\n" +
               "\tconfigFile  : " + configFile;
    }

    public DatabaseName getDatabaseName()
    {
        return databaseName;
    }
}

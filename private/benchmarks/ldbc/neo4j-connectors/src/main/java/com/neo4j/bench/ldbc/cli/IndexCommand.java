/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.utils.Utils;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;

import static java.lang.String.format;

@Command(
        name = "index",
        description = "(Re)indexes a Neo4j store with the necessary indexes for LDBC benchmark" )
public class IndexCommand implements Runnable
{
    public static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Target Neo4j database directory",
            title = "DB Directory",
            required = true )
    private File dbDir;

    public static final String CMD_WITH_UNIQUE = "--with-unique";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WITH_UNIQUE},
            description = "Create unique constraints for properties that should have unique values",
            title = "Create Unique Constraints",
            required = false )
    private boolean withUnique;

    public static final String CMD_WITH_MANDATORY = "--with-mandatory";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WITH_MANDATORY},
            description = "Create mandatory constraints for required properties",
            title = "Create Mandatory Constraints",
            required = false )
    private boolean withMandatory;

    public static final String CMD_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CONFIG},
            description = "Database configuration file",
            title = "DB Config",
            required = false )
    private File dbConfigurationFile;

    public static final String CMD_NEO4J_SCHEMA = "--neo4j-schema";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_SCHEMA},
            description = "(Optional - can be inferred) Schema of Neo4j store: NEO4J_REGULAR, NEO4J_DENSE_1",
            title = "Neo4j Schema",
            required = false )
    private String neo4jSchemaString;

    public static final String CMD_DROP_FIRST = "--drop-first";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DROP_FIRST},
            description = "Drop existing indexes & constraints before creating new ones",
            title = "Drop Any Existing Indexes/Constraints",
            required = false )
    private boolean dropFirst;

    @Override
    public void run()
    {
        System.out.println( format( "Target Neo4j Directory             : %s",
                (null == dbDir) ? null : dbDir.getAbsolutePath() ) );
        System.out.println( format( "Create Unique Constraints          : %s",
                withUnique ) );
        System.out.println( format( "Create Mandatory Constraints       : %s",
                withMandatory ) );
        System.out.println( format( "Drop Existing Indexes/Constraints  : %s",
                dropFirst ) );
        System.out.println( format( "Database Configuration File        : %s",
                (null == dbConfigurationFile) ? null : dbConfigurationFile.getAbsolutePath() ) );
        System.out.println( format( "Target Neo4j Schema                : %s",
                neo4jSchemaString ) );

        System.out.println( "*** Neo4j DB Properties ***" );
        try
        {
            System.out.println( Neo4jDb.configToString( dbConfigurationFile ) );
        }
        catch ( DbException e )
        {
            throw new RuntimeException( "Unable to read database configuration file to string", e );
        }
        System.out.println( "************************" );

        try
        {
            System.out.println( "Starting database..." );
            GraphDatabaseService db = Neo4jDb.newDb( dbDir, dbConfigurationFile );

            GraphMetadataProxy metadataProxy = GraphMetadataProxy.loadFrom( db );
            System.out.println( metadataProxy.toString() );

            Neo4jSchema neo4jSchema;
            if ( null == neo4jSchemaString )
            {
                System.out.println( "No Neo4j Schema provided, attempting to read schema from store" );
                neo4jSchema = metadataProxy.neo4jSchema();
                System.out.println( format( "Found: %s", neo4jSchema.name() ) );
            }
            else
            {
                neo4jSchema = Neo4jSchema.valueOf( neo4jSchemaString );
            }

            System.out.println( "Creating Indexes & Constraints" );
            long startTime = System.currentTimeMillis();

            LdbcIndexer indexer = new LdbcIndexer( neo4jSchema, withUnique, withMandatory, dropFirst );
            indexer.createTransactional( db );

            long runtime = System.currentTimeMillis() - startTime;
            System.out.println( format(
                    "Indexes built in: %d min, %d sec",
                    TimeUnit.MILLISECONDS.toMinutes( runtime ),
                    TimeUnit.MILLISECONDS.toSeconds( runtime )
                    - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

            System.out.printf( "Shutting down database..." );
            db.shutdown();
            System.out.println( "Done" );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Encountered error while indexing database", e );
        }
    }

    public static String[] buildArgs(
            File dbDir,
            File dbConfigurationFile,
            Neo4jSchema neo4jSchema,
            boolean withUnique,
            boolean withMandatory,
            boolean dropFirst )
    {
        String[] args = new String[]{
                "index",
                CMD_DB, dbDir.getAbsolutePath()
        };
        if ( null != dbConfigurationFile )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_CONFIG );
            args = Utils.copyArrayAndAddElement( args, dbConfigurationFile.getAbsolutePath() );
        }
        if ( null != neo4jSchema )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_NEO4J_SCHEMA );
            args = Utils.copyArrayAndAddElement( args, neo4jSchema.name() );
        }
        if ( withUnique )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WITH_UNIQUE );
        }
        if ( withMandatory )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WITH_MANDATORY );
        }
        if ( dropFirst )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_DROP_FIRST );
        }
        return args;
    }
}

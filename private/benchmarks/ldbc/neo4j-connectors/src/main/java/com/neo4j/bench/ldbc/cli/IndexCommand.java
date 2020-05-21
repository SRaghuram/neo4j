/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Command(
        name = "index",
        description = "(Re)indexes a Neo4j store with the necessary indexes for LDBC benchmark" )
public class IndexCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( IndexCommand.class );

    public static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
             name = {CMD_DB},
             description = "Target Neo4j database directory",
             title = "DB Directory" )
    @Required
    private File storeDir;

    public static final String CMD_WITH_UNIQUE = "--with-unique";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WITH_UNIQUE},
             description = "Create unique constraints for properties that should have unique values",
             title = "Create Unique Constraints" )
    private boolean withUnique;

    public static final String CMD_WITH_MANDATORY = "--with-mandatory";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WITH_MANDATORY},
             description = "Create mandatory constraints for required properties",
             title = "Create Mandatory Constraints" )
    private boolean withMandatory;

    public static final String CMD_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_CONFIG},
             description = "Database configuration file",
             title = "DB Config" )
    private File dbConfigurationFile;

    public static final String CMD_NEO4J_SCHEMA = "--neo4j-schema";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_SCHEMA},
             description = "(Optional - can be inferred) Schema of Neo4j store: NEO4J_REGULAR, NEO4J_DENSE_1",
             title = "Neo4j Schema" )
    private String neo4jSchemaString;

    public static final String CMD_DROP_FIRST = "--drop-first";
    @Option( type = OptionType.COMMAND,
             name = {CMD_DROP_FIRST},
             description = "Drop existing indexes & constraints before creating new ones",
             title = "Drop Any Existing Indexes/Constraints" )
    private boolean dropFirst;

    @Override
    public void run()
    {
        LOG.debug( format( "Target Neo4j Directory             : %s",
                           (null == storeDir) ? null : storeDir.getAbsolutePath() ) );
        LOG.debug( format( "Create Unique Constraints          : %s",
                           withUnique ) );
        LOG.debug( format( "Create Mandatory Constraints       : %s",
                           withMandatory ) );
        LOG.debug( format( "Drop Existing Indexes/Constraints  : %s",
                           dropFirst ) );
        LOG.debug( format( "Database Configuration File        : %s",
                           (null == dbConfigurationFile) ? null : dbConfigurationFile.getAbsolutePath() ) );
        LOG.debug( format( "Target Neo4j Schema                : %s",
                           neo4jSchemaString ) );

        LOG.debug( "*** Neo4j DB Properties ***" );
        LOG.debug( Neo4jDb.configToString( dbConfigurationFile ) );
        LOG.debug( "************************" );

        try
        {
            LOG.debug( "Starting database..." );
            DatabaseManagementService managementService = Neo4jDb.newDb( storeDir, dbConfigurationFile );

            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            GraphMetadataProxy metadataProxy = GraphMetadataProxy.loadFrom( db );
            LOG.debug( metadataProxy.toString() );

            Neo4jSchema neo4jSchema;
            if ( null == neo4jSchemaString )
            {
                LOG.debug( "No Neo4j Schema provided, attempting to read schema from store" );
                neo4jSchema = metadataProxy.neo4jSchema();
                LOG.debug( format( "Found: %s", neo4jSchema.name() ) );
            }
            else
            {
                neo4jSchema = Neo4jSchema.valueOf( neo4jSchemaString );
            }

            LOG.debug( "Creating Indexes & Constraints" );
            long startTime = System.currentTimeMillis();

            LdbcIndexer indexer = new LdbcIndexer( neo4jSchema, withUnique, withMandatory, dropFirst );
            indexer.createTransactional( db );

            long runtime = System.currentTimeMillis() - startTime;
            LOG.debug( format(
                    "Indexes built in: %d min, %d sec",
                    TimeUnit.MILLISECONDS.toMinutes( runtime ),
                    TimeUnit.MILLISECONDS.toSeconds( runtime )
                    - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

            LOG.debug( "Shutting down database..." );
            managementService.shutdown();
            LOG.debug( "Done" );
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

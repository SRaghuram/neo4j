/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.bench.ldbc.utils.Utils;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Command(
        name = "inspect",
        description = "Reports which LDBC schema variant is in the store" )
public class InspectCommand implements Runnable
{
    public static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Target Neo4j database directory",
            title = "DB Directory" )
    @Required
    private File storeDir;

    public static final String CMD_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_CONFIG},
             description = "Database configuration file",
             title = "DB Config" )
    private File dbConfigurationFile;

    @Override
    public void run()
    {
        System.out.println( format( "Target Neo4j Directory             : %s",
                                    (null == storeDir) ? null : storeDir.getAbsolutePath() ) );
        System.out.println( format( "Database Configuration File        : %s",
                                    (null == dbConfigurationFile) ? null : dbConfigurationFile.getAbsolutePath() ) );

        System.out.println( "*** Neo4j DB Properties ***" );
        System.out.println( Neo4jDb.configToString( dbConfigurationFile ) );
        System.out.println( "************************" );

        try
        {
            System.out.println( "Starting database..." );
            DatabaseManagementService managementService = Neo4jDb.newDb( storeDir, dbConfigurationFile );
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            GraphMetadataProxy metadataProxy = GraphMetadataProxy.loadFrom( db );
            System.out.println( metadataProxy.toString() );
            managementService.shutdown();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Encountered error while inspecting database", e );
        }
    }

    public static String[] buildArgs(
            File dbDir,
            File dbConfigurationFile )
    {
        String[] args = new String[]{
                "inspect",
                CMD_DB, dbDir.getAbsolutePath()
        };
        if ( null != dbConfigurationFile )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_CONFIG );
            args = Utils.copyArrayAndAddElement( args, dbConfigurationFile.getAbsolutePath() );
        }
        return args;
    }
}

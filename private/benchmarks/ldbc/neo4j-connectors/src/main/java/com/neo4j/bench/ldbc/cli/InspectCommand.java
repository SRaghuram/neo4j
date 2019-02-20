/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.utils.Utils;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;

import static java.lang.String.format;

@Command(
        name = "inspect",
        description = "Reports which LDBC schema variant is in the store" )
public class InspectCommand implements Runnable
{
    public static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Target Neo4j database directory",
            title = "DB Directory",
            required = true )
    private File dbDir;

    public static final String CMD_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CONFIG},
            description = "Database configuration file",
            title = "DB Config",
            required = false )
    private File dbConfigurationFile;

    @Override
    public void run()
    {
        System.out.println( format( "Target Neo4j Directory             : %s",
                (null == dbDir) ? null : dbDir.getAbsolutePath() ) );
        System.out.println( format( "Database Configuration File        : %s",
                (null == dbConfigurationFile) ? null : dbConfigurationFile.getAbsolutePath() ) );

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
            db.shutdown();
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

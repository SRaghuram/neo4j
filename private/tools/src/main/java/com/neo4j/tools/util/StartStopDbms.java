/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.util;

import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@CommandLine.Command( name = "start-stop-dbms", description = "Starts and stops a dbms and a db" )
public class StartStopDbms implements Callable<Object>
{
    @CommandLine.Parameters( index = "0", description = "Home directory." )
    private Path homeDirectory;

    @CommandLine.Option( names = "--database", description = "Database name." )
    private String databaseName = DEFAULT_DATABASE_NAME;

    @CommandLine.Option( names = "--allow-upgrade", description = "Whether or not to allow automatic upgrade." )
    private boolean allowUpgrade;

    @CommandLine.Option( names = "--record-format", description = "Record format to use." )
    private String recordFormat = record_format.defaultValue();

    @Override
    public Object call() throws Exception
    {
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder( homeDirectory )
                .setConfig( allow_upgrade, allowUpgrade )
                .setConfig( record_format, recordFormat )
                .build();
        try
        {
            GraphDatabaseService db = dbms.database( databaseName );
            // Just start an innocent tx for the sake of knowing that everything is started and OK with the selected database
            db.beginTx().rollback();
        }
        finally
        {
            dbms.shutdown();
        }
        return null;
    }

    public static void main( String[] args )
    {
        System.exit( new CommandLine( new StartStopDbms() ).execute( args ) );
    }
}

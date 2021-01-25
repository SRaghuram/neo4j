/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;

public class StopStartManagerCommand extends DatabaseManagerCommand
{
    public StopStartManagerCommand( DatabaseManagementService dbms, String databaseName )
    {
        super( dbms, databaseName );
    }

    @Override
    void execute( DatabaseManagementService dbms, String databaseName ) throws DatabaseExistsException, DatabaseNotFoundException
    {
        dbms.shutdownDatabase( databaseName );
        dbms.startDatabase( databaseName );
    }
}

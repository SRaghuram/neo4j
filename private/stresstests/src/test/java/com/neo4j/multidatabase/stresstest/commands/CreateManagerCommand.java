/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;

public class CreateManagerCommand extends DatabaseManagerCommand
{
    public CreateManagerCommand( DatabaseManagementService dbms, String databaseName )
    {
        super( dbms, databaseName );
    }

    @Override
    void execute( DatabaseManagementService dbms, String databaseName ) throws DatabaseExistsException
    {
        dbms.createDatabase( databaseName );
    }
}

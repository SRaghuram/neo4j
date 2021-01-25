/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;

public abstract class DatabaseManagerCommand
{
    private final DatabaseManagementService dbms;
    private final String databaseName;

    DatabaseManagerCommand( DatabaseManagementService dbms, String databaseName )
    {
        this.dbms = dbms;
        this.databaseName = databaseName;
    }

    public final void execute() throws DatabaseExistsException, DatabaseNotFoundException
    {
        execute( dbms, databaseName );
    }

    abstract void execute( DatabaseManagementService dbms, String databaseName ) throws DatabaseExistsException, DatabaseNotFoundException;
}

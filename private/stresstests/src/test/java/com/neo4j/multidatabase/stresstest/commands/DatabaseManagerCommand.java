/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;

public abstract class DatabaseManagerCommand
{
    private final DatabaseManager<?> manager;
    private final DatabaseId databaseId;

    DatabaseManagerCommand( DatabaseManager<?> manager, DatabaseId databaseId )
    {
        this.manager = manager;
        this.databaseId = databaseId;
    }

    public final void execute() throws DatabaseExistsException, DatabaseNotFoundException
    {
        execute( manager, databaseId );
    }

    abstract void execute( DatabaseManager<?> manager, DatabaseId databaseId ) throws DatabaseExistsException, DatabaseNotFoundException;
}

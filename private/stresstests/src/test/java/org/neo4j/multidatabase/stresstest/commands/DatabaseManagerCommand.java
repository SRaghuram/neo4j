/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.dbms.database.StandaloneDatabaseContext;

public abstract class DatabaseManagerCommand
{
    private final DatabaseManager<?> manager;
    private final String databaseName;

    DatabaseManagerCommand( DatabaseManager<?> manager, String databaseName )
    {
        this.manager = manager;
        this.databaseName = databaseName;
    }

    public final void execute() throws DatabaseExistsException, DatabaseNotFoundException
    {
        execute( manager, databaseName );
    }

    abstract void execute( DatabaseManager<?> manager, String databaseName ) throws DatabaseExistsException, DatabaseNotFoundException;
}

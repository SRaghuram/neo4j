/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.database.DatabaseManager;

public abstract class DatabaseManagerCommand
{
    private final DatabaseManager manager;
    private final String databaseName;

    DatabaseManagerCommand( DatabaseManager manager, String databaseName )
    {
        this.manager = manager;
        this.databaseName = databaseName;
    }

    public final void execute()
    {
        execute( manager, databaseName );
    }

    abstract void execute( DatabaseManager manager, String databaseName );
}

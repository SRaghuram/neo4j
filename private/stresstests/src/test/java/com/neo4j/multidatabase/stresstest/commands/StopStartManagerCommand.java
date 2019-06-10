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

public class StopStartManagerCommand extends DatabaseManagerCommand
{
    public StopStartManagerCommand( DatabaseManager<?> manager, DatabaseId databaseId )
    {
        super( manager, databaseId );
    }

    @Override
    void execute( DatabaseManager<?> manager, DatabaseId databaseId ) throws DatabaseExistsException, DatabaseNotFoundException
    {
        manager.stopDatabase( databaseId );
        manager.startDatabase( databaseId );
    }
}

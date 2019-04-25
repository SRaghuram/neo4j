/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseId;

public class DropManagerCommand extends DatabaseManagerCommand
{
    public DropManagerCommand( DatabaseManager<?> manager, DatabaseId databaseId )
    {
        super( manager, databaseId );
    }

    @Override
    void execute( DatabaseManager<?> manager, DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        manager.dropDatabase( databaseId );
    }
}

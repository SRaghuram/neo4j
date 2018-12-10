/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.database.DatabaseManager;

public class CreateManagerCommand extends DatabaseManagerCommand
{
    public CreateManagerCommand( DatabaseManager manager, String databaseName )
    {
        super( manager, databaseName );
    }

    @Override
    void execute( DatabaseManager manager, String databaseName )
    {
        manager.createDatabase( databaseName );
    }
}

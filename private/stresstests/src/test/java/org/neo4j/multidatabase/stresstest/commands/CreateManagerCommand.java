/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;

public class CreateManagerCommand extends DatabaseManagerCommand
{
    public CreateManagerCommand( DatabaseManager<?> manager, String databaseName )
    {
        super( manager, databaseName );
    }

    @Override
    void execute( DatabaseManager<?> manager, String databaseName ) throws DatabaseExistsException
    {
        manager.createDatabase( databaseName );
    }
}

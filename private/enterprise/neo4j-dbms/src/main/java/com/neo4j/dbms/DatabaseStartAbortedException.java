/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;

public class DatabaseStartAbortedException extends Exception
{
    public DatabaseStartAbortedException( DatabaseId databaseId )
    {
        super( format( "Database %s was stopped before it finished starting!", databaseId.name() ) );
    }
}

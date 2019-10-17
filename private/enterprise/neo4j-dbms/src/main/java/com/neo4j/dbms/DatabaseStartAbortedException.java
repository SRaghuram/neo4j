/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;


public class DatabaseStartAbortedException extends Exception
{
    public DatabaseStartAbortedException( String message )
    {
        super( message );
    }
}

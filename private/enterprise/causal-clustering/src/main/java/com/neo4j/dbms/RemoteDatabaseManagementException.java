/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.dbms.api.DatabaseManagementException;

/**
 * Simple catch all exception used for wrapping the deserialized messages of database
 * management exceptions which take place on other instances in a cluster.
 * The (de)serialization process removes the original exception type, cause and any stack.
 */
public class RemoteDatabaseManagementException extends DatabaseManagementException
{
    public RemoteDatabaseManagementException( String message )
    {
        super( message );
    }
}

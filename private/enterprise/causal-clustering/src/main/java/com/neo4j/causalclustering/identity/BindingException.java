/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

class BindingException extends Exception
{
    BindingException( String message )
    {
        super( message );
    }

    BindingException( String message, Throwable cause )
    {
        super( message, cause );
    }
}

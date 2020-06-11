/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

public class ConnectionDisruptedException extends IllegalStateException
{
    ConnectionDisruptedException( Throwable cause )
    {
        super( "Subprocess connection disrupted", cause );
    }
}

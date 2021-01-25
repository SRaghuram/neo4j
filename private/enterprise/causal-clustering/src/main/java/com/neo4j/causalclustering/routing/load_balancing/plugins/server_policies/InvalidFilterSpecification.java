/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

public class InvalidFilterSpecification extends Exception
{
    InvalidFilterSpecification( String message )
    {
        super( message );
    }

    InvalidFilterSpecification( String message, NumberFormatException cause )
    {
        super( message, cause );
    }
}

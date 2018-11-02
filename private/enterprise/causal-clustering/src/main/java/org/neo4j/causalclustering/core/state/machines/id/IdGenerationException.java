/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.neo4j.graphdb.TransactionFailureException;

public class IdGenerationException extends TransactionFailureException
{
    IdGenerationException( Throwable cause )
    {
        super( "Failed to generate record id", cause );
    }
}

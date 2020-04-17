/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import org.neo4j.kernel.api.exceptions.Status;

public class Exceptions
{
    public static RuntimeException transform( Status defaultStatus, Throwable t )
    {
        var unwrapped = reactor.core.Exceptions.unwrap( t );
        String message = unwrapped.getMessage();

        // preserve the original exception if possible
        // or try to preserve  at least the original status
        if ( unwrapped instanceof Status.HasStatus )
        {
            if ( unwrapped instanceof RuntimeException )
            {
                return (RuntimeException) unwrapped;
            }

            return new FabricException( ((Status.HasStatus) unwrapped).status(), message, unwrapped );
        }

        return new FabricException( defaultStatus, message, unwrapped );
    }
}

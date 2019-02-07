/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;

import java.util.function.Predicate;

public class IsStoreCopyFailure implements Predicate<Throwable>
{
    private final String expectedMessage;

    public IsStoreCopyFailure( Enum<?> expectedStatus )
    {
        this.expectedMessage = expectedStatus.toString();
    }

    @Override
    public boolean test( Throwable ex )
    {
        if ( ex == null )
        {
            return false;
        }

        if ( ex instanceof StoreCopyFailedException )
        {
            String message = ex.getMessage();
            return message != null && message.contains( expectedMessage );
        }

        return test( ex.getCause() );
    }
}

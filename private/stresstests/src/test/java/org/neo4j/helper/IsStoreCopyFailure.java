/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import java.util.function.Predicate;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;

import static java.util.Objects.requireNonNull;

public class IsStoreCopyFailure implements Predicate<Throwable>
{
    private final CatchupResult expectedResult;

    public IsStoreCopyFailure( CatchupResult expectedResult )
    {
        this.expectedResult = requireNonNull( expectedResult );
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
            return message != null && message.contains( expectedResult.toString() );
        }

        return test( ex.getCause() );
    }
}

/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.helper;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;

import java.util.Set;
import java.util.function.Predicate;

public class IsStoreCopyFailure implements Predicate<Throwable>
{
    private static final Set<Class<? extends Throwable>> STORE_COPY_EXCEPTION_TYPES = Set.of(
            StoreCopyFailedException.class, StoreIdDownloadFailedException.class );

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

        if ( isStoreCopyFailure( ex ) )
        {
            String message = ex.getMessage();
            return message != null && message.contains( expectedMessage );
        }

        return test( ex.getCause() );
    }

    private static boolean isStoreCopyFailure( Throwable ex )
    {
        return STORE_COPY_EXCEPTION_TYPES.stream().anyMatch( type -> type.isInstance( ex ) );
    }
}

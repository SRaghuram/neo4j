/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.kernel.api.exceptions.Status;

public class Utils
{
    static RemoteBookmark convertBookmark( Bookmark bookmark )
    {
        if ( bookmark == null )
        {
            return null;
        }

        // Even though the internal state of Driver's bookmark is a set,
        // currently the set size in a bookmark received from a server is always 1
        // assuming that will simplify bookmark handling a lot
        // if this is changed by the driver, it should fail our tests
        if ( bookmark.values().size() != 1 )
        {
            throw new IllegalArgumentException( "Unexpected bookmark format received from a remote" );
        }

        String serialisedBookmark = bookmark.values().stream().findAny().get();
        return new RemoteBookmark( serialisedBookmark );
    }

    static FabricException translateError( Neo4jException driverException )
    {
        // only user errors ( typically wrongly written query ) keep the original status code
        // server errors get a special status to distinguish them from error occurring on the local server
        if ( driverException instanceof ClientException )
        {
            var serverCode = Status.Code.all().stream().filter( code -> code.code().serialize().equals( driverException.code() ) ).findAny();

            if ( serverCode.isEmpty() )
            {
                return genericRemoteFailure( driverException );
            }

            return new FabricException( serverCode.get(), driverException.getMessage(), driverException );
        }

        return genericRemoteFailure( driverException );
    }

    private static FabricException genericRemoteFailure( Neo4jException driverException )
    {
        return new FabricException( Status.Fabric.RemoteExecutionFailed,
                String.format( "Remote execution failed with code %s and message '%s'", driverException.code(), driverException.getMessage() ),
                driverException );
    }
}

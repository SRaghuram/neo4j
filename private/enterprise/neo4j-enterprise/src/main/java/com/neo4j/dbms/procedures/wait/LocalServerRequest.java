/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.catchup.v4.info.InfoProvider;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

class LocalServerRequest extends ServerRequest
{
    private final InfoProvider infoProvider;

    LocalServerRequest( long txId, ServerId myself, SocketAddress socketAddress,
            InfoProvider infoProvider, NamedDatabaseId databaseId,
            Log log )
    {
        super( txId, myself, socketAddress, databaseId, log );
        this.infoProvider = infoProvider;
    }

    @Override
    public InfoResponse getInfo( NamedDatabaseId databaseId ) throws UnavailableException
    {
        return infoProvider.getInfo( databaseId );
    }
}

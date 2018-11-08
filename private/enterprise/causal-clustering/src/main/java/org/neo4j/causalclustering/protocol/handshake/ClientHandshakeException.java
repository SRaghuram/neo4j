/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.helpers.collection.Pair;

public class ClientHandshakeException extends Exception
{
    public ClientHandshakeException( String message )
    {
        super( message );
    }

    public ClientHandshakeException( String message, @Nullable Protocol.ApplicationProtocol negotiatedApplicationProtocol,
            List<Pair<String,Optional<Protocol.ModifierProtocol>>> negotiatedModifierProtocols )
    {
        super( message + " Negotiated application protocol: " + negotiatedApplicationProtocol +
                " Negotiated modifier protocols: " + negotiatedModifierProtocols );
    }
}

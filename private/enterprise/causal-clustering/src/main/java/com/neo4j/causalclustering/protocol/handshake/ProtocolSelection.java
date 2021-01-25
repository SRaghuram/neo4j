/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.Protocol;

import java.util.Collections;
import java.util.Set;

public abstract class ProtocolSelection<U extends Comparable<U>, T extends Protocol<U>>
{
    private final String identifier;
    private final Set<U> versions;

    public ProtocolSelection( String identifier, Set<U> versions )
    {
        this.identifier = identifier;
        this.versions = Collections.unmodifiableSet( versions );
    }

    public String identifier()
    {
        return identifier;
    }

    public Set<U> versions()
    {
        return versions;
    }
}

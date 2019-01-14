/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Collections;
import java.util.Set;

import org.neo4j.causalclustering.protocol.Protocol;

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

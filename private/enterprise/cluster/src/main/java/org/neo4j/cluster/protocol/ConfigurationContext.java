/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;

public interface ConfigurationContext
{
    InstanceId getMyId();

    List<URI> getMemberURIs();

    URI boundAt();

    Map<InstanceId,URI> getMembers();

    InstanceId getCoordinator();

    URI getUriForId( InstanceId id );

    InstanceId getIdForUri( URI uri );

    boolean isMe( InstanceId server );
}

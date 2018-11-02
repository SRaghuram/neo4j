/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

import org.neo4j.cluster.InstanceId;

/**
 * Election API.
 *
 * @see ElectionState
 * @see ElectionMessage
 */
public interface Election
{
    void demote( InstanceId node );

    /**
     * Asks an election to be performed for all currently known roles, regardless of someone holding that role
     * currently. It is allowed for the same instance to be reelected.
     */
    void performRoleElections();
}

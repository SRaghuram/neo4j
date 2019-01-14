/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.member;

import java.net.URI;

import org.neo4j.storageengine.api.StoreId;

/**
 * This can be used to signal that a cluster member can now actively
 * participate with a given role, accompanied by a URI for accessing that role.
 */
public interface ClusterMemberAvailability
{
    /**
     * When a member has finished a transition to a particular role, i.e. master or slave,
     * then it should call this which will broadcast the new status to the cluster.
     *
     * @param role
     */
    void memberIsAvailable( String role, URI roleUri, StoreId storeId );

    /**
     * When a member is no longer available in a particular role it should call this
     * to announce it to the other members of the cluster.
     *
     * @param role
     */
    void memberIsUnavailable( String role );
}

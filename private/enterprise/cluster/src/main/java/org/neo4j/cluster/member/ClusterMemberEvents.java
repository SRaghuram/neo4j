/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.member;

/**
 * Register listeners here to get callbacks when important cluster events happen, such as elections,
 * availability/unavailability of a member as a particular role, and member failure/recovery.
 */
public interface ClusterMemberEvents
{
    void addClusterMemberListener( ClusterMemberListener listener );

    void removeClusterMemberListener( ClusterMemberListener listener );
}

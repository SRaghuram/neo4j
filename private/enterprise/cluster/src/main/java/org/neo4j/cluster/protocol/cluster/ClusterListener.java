/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

/**
 * Listener interface for cluster configuration changes. Register instances
 * of this interface with {@link Cluster}
 */
public interface ClusterListener
{
    /**
     * When I enter the cluster as a member, this callback notifies me
     * that this has been agreed upon by the entire cluster.
     *
     * @param clusterConfiguration
     */
    void enteredCluster( ClusterConfiguration clusterConfiguration );

    /**
     * When I leave the cluster, this callback notifies me
     * that this has been agreed upon by the entire cluster.
     */
    void leftCluster();

    /**
     * When another instance joins as a member, this callback is invoked
     *
     * @param member
     */
    void joinedCluster( InstanceId instanceId, URI member );

    /**
     * When another instance leaves the cluster, this callback is invoked.
     * Implicitly, any roles that this member had, are revoked.
     *
     * @param member
     */
    void leftCluster( InstanceId instanceId, URI member );

    /**
     * When a member (including potentially myself) has been elected to a particular role, this callback is invoked.
     * Combine this callback with the leftCluster to keep track of current set of role->member mappings.
     *
     * @param role
     * @param electedMember
     */
    void elected( String role, InstanceId instanceId, URI electedMember );

    void unelected( String role, InstanceId instanceId, URI electedMember );

    abstract class Adapter
            implements ClusterListener
    {
        @Override
        public void enteredCluster( ClusterConfiguration clusterConfiguration )
        {
        }

        @Override
        public void joinedCluster( InstanceId instanceId, URI member )
        {
        }

        @Override
        public void leftCluster( InstanceId instanceId, URI member )
        {
        }

        @Override
        public void leftCluster()
        {
        }

        @Override
        public void elected( String role, InstanceId instanceId, URI electedMember )
        {
        }

        @Override
        public void unelected( String role, InstanceId instanceId, URI electedMember )
        {
        }
    }
}

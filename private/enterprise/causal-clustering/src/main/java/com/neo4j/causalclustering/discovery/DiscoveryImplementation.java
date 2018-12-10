/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

/**
 * Enum of the production discovery service implementations users may choose between when configuring their cluster.
 * Akka discovery supports encrypting discovery level traffic, as well as simply encrypting intra-cluster Raft communication.
 * @see HazelcastDiscoveryServiceFactory
 * @see AkkaDiscoveryServiceFactory
 */
public enum DiscoveryImplementation
{
    HAZELCAST,
    AKKA
}

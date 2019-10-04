/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery.akka.monitoring;

public enum ReplicatedDataIdentifier
{
    METADATA( "member-data" ),
    CLUSTER_ID( "cluster-id-per-db-name" ),
    DIRECTORY( "per-db-leader-name" );

    private final String keyName;

    ReplicatedDataIdentifier( String keyName )
    {
        this.keyName = keyName;
    }

    public String keyName()
    {
        return keyName;
    };
}

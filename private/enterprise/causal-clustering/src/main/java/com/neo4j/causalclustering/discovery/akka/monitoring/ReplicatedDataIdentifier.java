/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.monitoring;

public enum ReplicatedDataIdentifier
{
    METADATA( "member-data" ),
    RAFT_ID_PUBLISHER( "raft-id-published-by-member" ),
    DIRECTORY( "per-db-leader-name" ),
    DATABASE_STATE( "member-db-state" ),
    RAFT_MEMBER_MAPPING( "raft-member-mapping" );

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

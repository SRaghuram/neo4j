/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.monitoring;

public enum ReplicatedDataIdentifier
{
    METADATA( "member-data" ), // appears as neo4j.causal_clustering.core.discovery.replicated_data.member_data
    RAFT_ID_PUBLISHER( "raft-id-published-by-member" ), // neo4j.causal_clustering.core.discovery.replicated_data.raft_id_published_by_member
    DIRECTORY( "per-db-leader-name" ), // neo4j.causal_clustering.core.discovery.replicated_data.per_db_leader_name
    DATABASE_STATE( "member-db-state" ), // neo4j.causal_clustering.core.discovery.replicated_data.member_db_state
    RAFT_MEMBER_MAPPING( "raft-member-mapping" ); // neo4j.causal_clustering.core.discovery.replicated_data.raft_member_mapping

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

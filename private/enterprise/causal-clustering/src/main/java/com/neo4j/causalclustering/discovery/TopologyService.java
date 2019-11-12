/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.dbms.DatabaseStateChangedListener;

import java.util.Map;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Provides a read-only service for the eventually consistent topology information.
 */
public interface TopologyService extends Lifecycle, DatabaseStateChangedListener
{
    void onDatabaseStart( DatabaseId databaseId );

    void onDatabaseStop( DatabaseId databaseId );

    MemberId memberId();

    Map<MemberId,CoreServerInfo> allCoreServers();

    Map<MemberId,ReadReplicaInfo> allReadReplicas();

    DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId );

    DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId );

    SocketAddress lookupCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException;

    RoleInfo lookupRole( DatabaseId databaseId, MemberId memberId );

    DatabaseState lookupDatabaseState( DatabaseId databaseId, MemberId memberId );

    Map<MemberId,DatabaseState> allCoreStatesForDatabase( DatabaseId databaseId );

    Map<MemberId,DatabaseState> allReadReplicaStatesForDatabase( DatabaseId databaseId );
}

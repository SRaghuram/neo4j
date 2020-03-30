package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

@FunctionalInterface
interface RaftMembershipResolver
{
    RaftMembership membersFor( NamedDatabaseId namedDatabaseId );
}

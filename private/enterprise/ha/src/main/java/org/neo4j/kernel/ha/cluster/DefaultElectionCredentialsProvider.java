/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.election.ElectionCredentials;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.kernel.ha.HighAvailabilityMemberInfoProvider;
import org.neo4j.kernel.impl.core.LastTxIdGetter;

/**
 * ElectionCredentialsProvider that provides the server id, latest txId and current role status as credentials for
 * elections.
 */
public class DefaultElectionCredentialsProvider
    implements ElectionCredentialsProvider
{
    private final InstanceId serverId;
    private final LastTxIdGetter lastTxIdGetter;
    private final HighAvailabilityMemberInfoProvider masterInfo;

    public DefaultElectionCredentialsProvider( InstanceId serverId, LastTxIdGetter lastTxIdGetter,
                                               HighAvailabilityMemberInfoProvider masterInfo )
    {
        this.serverId = serverId;
        this.lastTxIdGetter = lastTxIdGetter;
        this.masterInfo = masterInfo;
    }

    @Override
    public ElectionCredentials getCredentials( String role )
    {
        if ( masterInfo.getHighAvailabilityMemberState().isEligibleForElection() )
        {
            return new DefaultElectionCredentials(
                    serverId.toIntegerIndex(),
                    lastTxIdGetter.getLastTxId(),
                    isMasterOrToMaster() );
        }
        return new NotElectableElectionCredentials();
    }

    private boolean isMasterOrToMaster()
    {
        return masterInfo.getHighAvailabilityMemberState() == HighAvailabilityMemberState.MASTER ||
                masterInfo.getHighAvailabilityMemberState() == HighAvailabilityMemberState.TO_MASTER;
    }
}

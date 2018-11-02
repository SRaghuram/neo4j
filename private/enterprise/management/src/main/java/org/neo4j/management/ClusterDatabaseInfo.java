/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

/**
 * @deprecated high availability database/edition is deprecated in favour of causal clustering. It will be removed in next major release.
 */
@Deprecated
public class ClusterDatabaseInfo extends ClusterMemberInfo
{
    private long lastCommittedTxId;
    private long lastUpdateTime;

    public ClusterDatabaseInfo( ClusterMemberInfo memberInfo, long lastCommittedTxId, long lastUpdateTime )
    {
        super( memberInfo.getInstanceId(), memberInfo.isAvailable(), memberInfo.isAlive(), memberInfo.getHaRole(),
                memberInfo.getUris(), memberInfo.getRoles() );
        this.lastCommittedTxId = lastCommittedTxId;
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getLastCommittedTxId()
    {
        return lastCommittedTxId;
    }

    public long getLastUpdateTime()
    {
        return lastUpdateTime;
    }
}

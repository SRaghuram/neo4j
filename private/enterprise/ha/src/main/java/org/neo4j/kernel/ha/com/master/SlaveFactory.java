/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.StoreId;

public interface SlaveFactory
{
    Slave newSlave( LifeSupport life, ClusterMember clusterMember, String originHostNameOrIp, int originPort );

    void setStoreId( StoreId storeId );
}

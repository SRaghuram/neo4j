/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.slave;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

public interface MasterClientFactory
{
    MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
            Monitors monitors, StoreId storeId, LifeSupport life );
}

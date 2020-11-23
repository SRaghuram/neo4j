/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventListener;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;

public class EnterpriseDefaultDatabaseResolver extends CommunityDefaultDatabaseResolver implements ReplicatedDatabaseEventListener
{
    public EnterpriseDefaultDatabaseResolver( Config config, Supplier<GraphDatabaseService> systemDbSupplier )
    {
        super( config, systemDbSupplier );
    }

    @Override
    public void transactionCommitted( long txId )
    {
        super.clearCache();
    }

    @Override
    public void storeReplaced( long txId )
    {
        super.clearCache();
    }
}

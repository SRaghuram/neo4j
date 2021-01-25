/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;

public class ByIdInstanceSelector implements InstanceSelector
{
    private final int serverId;

    public ByIdInstanceSelector( int serverId )
    {
        this.serverId = serverId;
    }

    @Override
    public CcInstance select( Orchestrator orchestrator )
    {
        return orchestrator.getCcInstance( serverId );
    }

    @Override
    public String name()
    {
        return "InstanceSelector[serverId:" + serverId + "]";
    }
}

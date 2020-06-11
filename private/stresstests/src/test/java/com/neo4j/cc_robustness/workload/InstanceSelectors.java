/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;

public enum InstanceSelectors implements InstanceSelector
{
    any
            {
                @Override
                public CcInstance select( Orchestrator orchestrator )
                {
                    return orchestrator.getRandomCcInstance();
                }
            },
    leader
            {
                @Override
                public CcInstance select( Orchestrator orchestrator )
                {
                    return orchestrator.getLeaderInstance();
                }
            },
    followers
            {
                @Override
                public CcInstance select( Orchestrator orchestrator )
                {
                    return orchestrator.getRandomFollowerCcInstance();
                }
            },
    none
            {
                @Override
                public CcInstance select( Orchestrator orchestrator )
                {
                    return null;
                }
            };

    public static InstanceSelector selector( String string )
    {
        try
        {
            int serverId = Integer.parseInt( string );
            return new ByIdInstanceSelector( serverId );
        }
        catch ( NumberFormatException e )
        {
            return valueOf( string );
        }
    }
}

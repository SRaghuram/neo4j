/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

public enum ShutdownTypeSelector
{
    clean
            {
                @Override
                public ShutdownType select()
                {
                    return ShutdownType.clean;
                }
            },
    harsh
            {
                @Override
                public ShutdownType select()
                {
                    return ShutdownType.harsh;
                }
            },
    random
            {
                @Override
                public ShutdownType select()
                {
                    return System.currentTimeMillis() % 2 == 0 ? ShutdownType.clean : ShutdownType.harsh;
                }
            };

    public abstract ShutdownType select();
}

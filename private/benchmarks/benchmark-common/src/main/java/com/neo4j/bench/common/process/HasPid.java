/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import java.lang.management.ManagementFactory;

public interface HasPid
{
    /**
     * NOTE: Below this line stolen verbatim from JMH
     * <p>
     * Gets PID of the current JVM.
     *
     * @return PID.
     */
    static Pid getPid()
    {
        final String DELIM = "@";

        String name = ManagementFactory.getRuntimeMXBean().getName();

        if ( name != null )
        {
            int idx = name.indexOf( DELIM );

            if ( idx != -1 )
            {
                String str = name.substring( 0, name.indexOf( DELIM ) );
                try
                {
                    return new Pid( Long.parseLong( str ) );
                }
                catch ( NumberFormatException nfe )
                {
                    throw new IllegalStateException( "Process PID is not a number: " + str );
                }
            }
        }
        throw new IllegalStateException( "Unsupported PID format: " + name );
    }

    Pid pid();
}

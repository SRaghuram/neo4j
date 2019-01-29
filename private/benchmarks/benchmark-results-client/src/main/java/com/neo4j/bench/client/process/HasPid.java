package com.neo4j.bench.client.process;

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
    static long getPid()
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
                    return Long.valueOf( str );
                }
                catch ( NumberFormatException nfe )
                {
                    throw new IllegalStateException( "Process PID is not a number: " + str );
                }
            }
        }
        throw new IllegalStateException( "Unsupported PID format: " + name );
    }

    long pid();
}

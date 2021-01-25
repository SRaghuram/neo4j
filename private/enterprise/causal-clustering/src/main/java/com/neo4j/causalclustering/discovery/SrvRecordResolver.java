/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.stream.Stream;
import javax.naming.NamingException;

public abstract class SrvRecordResolver
{
    public abstract Stream<SrvRecord> resolveSrvRecord( String url ) throws NamingException;

    public Stream<SrvRecord> resolveSrvRecord( String service, String protocol, String hostname ) throws NamingException
    {
        String url = String.format( "_%s._%s.%s", service, protocol, hostname );

        return resolveSrvRecord( url );
    }

    public static class SrvRecord
    {
        public final int priority;
        public final int weight;
        public final int port;
        public final String host;

        private SrvRecord( int priority, int weight, int port, String host )
        {
            this.priority = priority;
            this.weight = weight;
            this.port = port;
            // Typically the SRV record has a trailing dot - if that is the case we should remove it
            this.host = host.charAt( host.length() - 1 ) == '.' ? host.substring( 0, host.length() - 1 ) : host;
        }

        public static SrvRecord parse( String input )
        {
            String[] parts = input.split( " " );
            return new SrvRecord(
                    Integer.parseInt( parts[0] ),
                    Integer.parseInt( parts[1] ),
                    Integer.parseInt( parts[2] ),
                    parts[3]
            );
        }
    }
}

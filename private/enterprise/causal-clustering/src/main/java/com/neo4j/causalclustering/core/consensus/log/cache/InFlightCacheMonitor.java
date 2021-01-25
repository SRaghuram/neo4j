/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

public interface InFlightCacheMonitor
{
    InFlightCacheMonitor VOID = new InFlightCacheMonitor()
    {
        @Override
        public void miss()
        {
        }

        @Override
        public void hit()
        {

        }

        @Override
        public void setMaxBytes( long maxBytes )
        {

        }

        @Override
        public void setTotalBytes( long totalBytes )
        {

        }

        @Override
        public void setMaxElements( int maxElements )
        {

        }

        @Override
        public void setElementCount( int elementCount )
        {

        }
    };

    void miss();

    void hit();

    void setMaxBytes( long maxBytes );

    void setTotalBytes( long totalBytes );

    void setMaxElements( int maxElements );

    void setElementCount( int elementCount );
}

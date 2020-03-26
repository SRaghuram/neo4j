/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import java.util.List;
import java.util.Objects;

import org.neo4j.configuration.helpers.SocketAddress;

public class Location
{
    private final long graphId;
    private final String databaseName;

    private Location( long graphId, String databaseName )
    {
        this.graphId = graphId;
        this.databaseName = databaseName;
    }

    public long getGraphId()
    {
        return graphId;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public static class Local extends Location
    {
        public Local( long id, String databaseName )
        {
            super( id, databaseName );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Local local = (Local) o;
            return Objects.equals( getGraphId(), local.getGraphId() ) && Objects.equals( getDatabaseName(), local.getDatabaseName() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( getGraphId(), getDatabaseName() );
        }
    }

    public static class Remote extends Location
    {
        private final RemoteUri uri;

        public Remote( long id, RemoteUri uri, String databaseName )
        {
            super( id, databaseName );
            this.uri = uri;
        }

        public RemoteUri getUri()
        {
            return uri;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Remote remote = (Remote) o;
            return Objects.equals( uri, remote.uri );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( uri );
        }
    }

    public static class RemoteUri
    {
        private final String scheme;
        private final List<SocketAddress> addresses;
        private final String query;

        public RemoteUri( String scheme, List<SocketAddress> addresses, String query )
        {
            this.scheme = scheme;
            this.addresses = addresses;
            this.query = query;
        }

        public String getScheme()
        {
            return scheme;
        }

        public List<SocketAddress> getAddresses()
        {
            return addresses;
        }

        public String getQuery()
        {
            return query;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RemoteUri remoteUri = (RemoteUri) o;
            return Objects.equals( scheme, remoteUri.scheme ) && Objects.equals( addresses, remoteUri.addresses ) && Objects.equals( query, remoteUri.query );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( scheme, addresses, query );
        }
    }
}

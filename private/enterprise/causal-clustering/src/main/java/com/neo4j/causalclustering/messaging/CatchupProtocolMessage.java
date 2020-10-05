/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.catchup.RequestMessageType;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

public abstract class CatchupProtocolMessage
{
    protected final RequestMessageType type;

    public CatchupProtocolMessage( RequestMessageType type )
    {
        this.type = type;
    }

    public final RequestMessageType messageType()
    {
        return type;
    }

    public abstract String describe();

    public abstract static class WithDatabaseId extends CatchupProtocolMessage
    {
        private final DatabaseId databaseId;

        protected WithDatabaseId( RequestMessageType type, DatabaseId databaseId )
        {
            super( type );
            this.databaseId = databaseId;
        }

        public final DatabaseId databaseId()
        {
            return databaseId;
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
            WithDatabaseId that = (WithDatabaseId) o;
            return type == that.type &&
                   Objects.equals( databaseId, that.databaseId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( type, databaseId );
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "{ " + type + ", " + databaseId + " }";
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName() + " for " + databaseId;
        }
    }
}

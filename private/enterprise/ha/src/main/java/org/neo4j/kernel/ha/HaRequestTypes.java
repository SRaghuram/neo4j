/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.RequestType;

public interface HaRequestTypes
{
    enum Type
    {
        // Order here is vital since it represents the request type (byte) ordinal which is communicated
        // as part of the HA protocol.
        ALLOCATE_IDS,
        CREATE_RELATIONSHIP_TYPE,
        ACQUIRE_EXCLUSIVE_LOCK,
        ACQUIRE_SHARED_LOCK,
        COMMIT,
        PULL_UPDATES,
        END_LOCK_SESSION,
        HANDSHAKE,
        COPY_STORE,
        COPY_TRANSACTIONS,
        NEW_LOCK_SESSION,
        PUSH_TRANSACTIONS,
        CREATE_PROPERTY_KEY,
        CREATE_LABEL;

        public boolean is( RequestType type )
        {
            return type.id() == ordinal();
        }
    }

    RequestType type( Type type );

    RequestType type( byte id );
}

/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

public enum IncludeMetadata
{
    all( true, true ),
    users( true, false ),
    roles( false, true );

    public final boolean addUsers;
    public final boolean addRoles;

    IncludeMetadata( boolean addUsers, boolean addRoles )
    {
        this.addUsers = addUsers;
        this.addRoles = addRoles;
    }
}

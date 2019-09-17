/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import org.neo4j.internal.kernel.api.security.AdminActionOnResource;

public interface AdminAccessMode
{
    boolean allows( AdminActionOnResource action );

    AdminAccessMode FULL = action -> true;
}

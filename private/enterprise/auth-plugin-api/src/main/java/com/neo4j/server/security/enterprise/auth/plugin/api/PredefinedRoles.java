/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.api;

import java.util.List;

/**
 * The role names of the built-in predefined roles of Neo4j.
 */
public final class PredefinedRoles
{
    public static final String ADMIN = "admin";
    public static final String ARCHITECT = "architect";
    public static final String PUBLISHER = "publisher";
    public static final String EDITOR = "editor";
    public static final String READER = "reader";
    public static final String PUBLIC = "PUBLIC";

    public static final List<String> roles = List.of( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER, PUBLIC );

    private PredefinedRoles()
    {

    }
}

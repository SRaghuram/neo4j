/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin.api;

/**
 * The role names of the built-in predefined roles of Neo4j.
 */
public class PredefinedRoles
{
    public static final String ADMIN = "admin";
    public static final String ARCHITECT = "architect";
    public static final String PUBLISHER = "publisher";
    public static final String EDITOR = "editor";
    public static final String READER = "reader";

    private PredefinedRoles()
    {
    }
}

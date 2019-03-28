/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;


public class PredefinedRolesBuilder implements RolesBuilder
{
    public static final WildcardPermission SYSTEM = new WildcardPermission( "system:*" );
    public static final WildcardPermission SCHEMA = new WildcardPermission( "database:*:*:schema" );
    public static final WildcardPermission TOKEN = new WildcardPermission( "database:*:*:token" );
    public static final WildcardPermission WRITE = new WildcardPermission( "database:*:write:graph" );
    public static final WildcardPermission READ = new WildcardPermission( "database:*:read:graph" );

    private static final Map<String,SimpleRole> innerRoles = staticBuildRoles();
    public static final Map<String,SimpleRole> roles = Collections.unmodifiableMap( innerRoles );

    private static Map<String,SimpleRole> staticBuildRoles()
    {
        Map<String,SimpleRole> roles = new ConcurrentHashMap<>( 4 );

        SimpleRole admin = new SimpleRole( ADMIN );
        admin.add( SYSTEM );
        admin.add( SCHEMA );
        admin.add( TOKEN );
        admin.add( WRITE );
        admin.add( READ );
        roles.put( ADMIN, admin );

        SimpleRole architect = new SimpleRole( ARCHITECT );
        architect.add( SCHEMA );
        architect.add( TOKEN );
        architect.add( WRITE );
        architect.add( READ );
        roles.put( ARCHITECT, architect );

        SimpleRole publisher = new SimpleRole( PUBLISHER );
        publisher.add( TOKEN );
        publisher.add( WRITE );
        publisher.add( READ );
        roles.put( PUBLISHER, publisher );

        SimpleRole editor = new SimpleRole( EDITOR );
        editor.add( WRITE );
        editor.add( READ );
        roles.put( EDITOR, editor );

        SimpleRole reader = new SimpleRole( READER );
        reader.add( READ );
        roles.put( READER, reader );

        return roles;
    }

    public static final RolePermissionResolver rolePermissionResolver = roleString ->
    {
        if ( roleString == null )
        {
            return Collections.emptyList();
        }
        SimpleRole role = roles.get( roleString );
        if ( role != null )
        {
            return role.getPermissions();
        }
        else
        {
            return Collections.emptyList();
        }
    };

    @Override
    public Map<String,SimpleRole> buildRoles()
    {
        return roles;
    }
}

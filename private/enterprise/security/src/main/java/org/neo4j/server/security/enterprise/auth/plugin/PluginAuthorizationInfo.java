/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;

public class PluginAuthorizationInfo extends SimpleAuthorizationInfo
{
    private PluginAuthorizationInfo( Set<String> roles )
    {
        super( roles );
    }

    public static PluginAuthorizationInfo create( AuthorizationInfo authorizationInfo )
    {
        return new PluginAuthorizationInfo( new LinkedHashSet<>( authorizationInfo.roles() ) );
    }
}

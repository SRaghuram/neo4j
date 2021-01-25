/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.LinkedHashSet;
import java.util.Set;

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

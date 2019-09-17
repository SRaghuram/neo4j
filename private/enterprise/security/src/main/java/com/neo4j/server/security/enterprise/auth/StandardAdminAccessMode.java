/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.AdminAccessMode;

import java.util.HashSet;

import org.neo4j.internal.kernel.api.security.AdminAction;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;

public class StandardAdminAccessMode implements AdminAccessMode
{
    private final HashSet<AdminActionOnResource> whitelist;
    private final HashSet<AdminActionOnResource> blacklist;

    StandardAdminAccessMode( HashSet<AdminActionOnResource> whitelist, HashSet<AdminActionOnResource> blacklist )
    {
        this.whitelist = whitelist;
        this.blacklist = blacklist;
    }

    @Override
    public boolean allows( AdminActionOnResource action )
    {
        return matches( whitelist, action ) && !matches( blacklist, action );
    }

    public static boolean matches( HashSet<AdminActionOnResource> actions, AdminActionOnResource action )
    {
        for ( AdminActionOnResource rule : actions )
        {
            if ( rule.matches( action ) )
            {
                return true;
            }
        }
        return false;
    }

    public static class Builder
    {
        HashSet<AdminActionOnResource> whitelist = new HashSet<>();
        HashSet<AdminActionOnResource> blacklist = new HashSet<>();

        public Builder full()
        {
            for ( AdminAction a : AdminAction.values() )
            {
                whitelist.add( new AdminActionOnResource( a, AdminActionOnResource.DatabaseScope.ALL ) );
            }
            return this;
        }

        public Builder allow( AdminActionOnResource action )
        {
            whitelist.add( action );
            return this;
        }

        public Builder deny( AdminActionOnResource action )
        {
            blacklist.add( action );
            return this;
        }

        public StandardAdminAccessMode build()
        {
            return new StandardAdminAccessMode( whitelist, blacklist );
        }
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * A logged in and authorized user.
 */
public class CommercialSecurityContext extends SecurityContext
{
    private final Set<String> roles;
    private final boolean isAdmin;

    public CommercialSecurityContext( AuthSubject subject, AccessMode mode, Set<String> roles, boolean isAdmin )
    {
        super( subject, mode );
        this.roles = roles;
        this.isAdmin = isAdmin;
    }

    @Override
    public boolean isAdmin()
    {
        return isAdmin;
    }

    @Override
    public CommercialSecurityContext authorize( PropertyKeyIdLookup propertyKeyIdLookup, String dbName )
    {
        return this;
    }

    @Override
    public CommercialSecurityContext withMode( AccessMode mode )
    {
        return new CommercialSecurityContext( subject, mode, roles, isAdmin );
    }

    /**
     * Get the roles of the authenticated user.
     */
    public Set<String> roles()
    {
        return roles;
    }

    /** Allows all operations. */
    public static final CommercialSecurityContext AUTH_DISABLED = authDisabled( AccessMode.Static.FULL );

    private static CommercialSecurityContext authDisabled( AccessMode mode )
    {
        return new CommercialSecurityContext( AuthSubject.AUTH_DISABLED, mode, Collections.emptySet(), true )
        {

            @Override
            public CommercialSecurityContext withMode( AccessMode mode )
            {
                return authDisabled( mode );
            }

            @Override
            public String description()
            {
                return "AUTH_DISABLED with " + mode().name();
            }

            @Override
            public String toString()
            {
                return defaultString( "enterprise-auth-disabled" );
            }
        };
    }
}

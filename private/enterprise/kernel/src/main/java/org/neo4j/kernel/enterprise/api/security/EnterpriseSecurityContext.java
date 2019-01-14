/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;
import java.util.function.ToIntFunction;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * A logged in and authorized user.
 */
public class EnterpriseSecurityContext extends SecurityContext
{
    private final Set<String> roles;
    private final boolean isAdmin;

    public EnterpriseSecurityContext( AuthSubject subject, AccessMode mode, Set<String> roles, boolean isAdmin )
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
    public EnterpriseSecurityContext authorize( ToIntFunction<String> propertyIdLookup, String dbName )
    {
        return this;
    }

    @Override
    public EnterpriseSecurityContext withMode( AccessMode mode )
    {
        return new EnterpriseSecurityContext( subject, mode, roles, isAdmin );
    }

    /**
     * Get the roles of the authenticated user.
     */
    public Set<String> roles()
    {
        return roles;
    }

    /** Allows all operations. */
    public static final EnterpriseSecurityContext AUTH_DISABLED = authDisabled( AccessMode.Static.FULL );

    private static EnterpriseSecurityContext authDisabled( AccessMode mode )
    {
        return new EnterpriseSecurityContext( AuthSubject.AUTH_DISABLED, mode, Collections.emptySet(), true )
        {

            @Override
            public EnterpriseSecurityContext withMode( AccessMode mode )
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

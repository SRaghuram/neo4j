/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.Segment;

/**
 * A logged in and authorized user.
 */
public class EnterpriseSecurityContext extends SecurityContext
{
    private final Set<String> roles;
    private final AdminAccessMode adminAccessMode;
    private static final AdminActionOnResource executeAdminProc = new AdminActionOnResource( PrivilegeAction.ADMIN_PROCEDURE, DatabaseScope.ALL, Segment.ALL );

    public EnterpriseSecurityContext( AuthSubject subject, AccessMode mode, Set<String> roles, AdminAccessMode adminAccessMode )
    {
        super( subject, mode );
        this.roles = roles;
        this.adminAccessMode = adminAccessMode;
    }

    @Override
    public boolean allowExecuteAdminProcedure( int procedureId )
    {
        return adminAccessMode.allows( executeAdminProc ) || mode.shouldBoostProcedure( procedureId );
    }

    @Override
    public boolean allowsAdminAction( AdminActionOnResource action )
    {
        return adminAccessMode.allows( action );
    }

    @Override
    public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
    {
        return this;
    }

    @Override
    public EnterpriseSecurityContext withMode( AccessMode mode )
    {
        return new EnterpriseSecurityContext( subject, mode, roles, adminAccessMode );
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
        return new EnterpriseSecurityContext( AuthSubject.AUTH_DISABLED, mode, Collections.emptySet(), AdminAccessMode.FULL )
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

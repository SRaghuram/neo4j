/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.Set;

import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.executor.FabricDatabaseAccess;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;

public class FabricDatabaseAccessImpl implements FabricDatabaseAccess
{
    private final FabricDatabaseManager databaseManager;

    public FabricDatabaseAccessImpl( FabricDatabaseManager databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    @Override
    public LoginContext maybeRestrictLoginContext( LoginContext originalContext, String databaseName )
    {
        boolean isConfiguredFabricDatabase = databaseManager.isFabricDatabase( databaseName );
        return isConfiguredFabricDatabase
               ? wrappedLoginContext( originalContext )
               : originalContext;
    }

    private LoginContext wrappedLoginContext( LoginContext originalContext )
    {
        // In Enterprise edition, LoginContext is always an instance of EnterpriseLoginContext.
        return new FabricLocalEnterpriseLoginContext( (EnterpriseLoginContext) originalContext );
    }

    private static class FabricLocalEnterpriseLoginContext implements EnterpriseLoginContext
    {
        private final EnterpriseLoginContext inner;

        private FabricLocalEnterpriseLoginContext( EnterpriseLoginContext inner )
        {
            this.inner = inner;
        }

        @Override
        public AuthSubject subject()
        {
            return inner.subject();
        }

        @Override
        public Set<String> roles()
        {
            return inner.roles();
        }

        @Override
        public EnterpriseSecurityContext authorize( LoginContext.IdLookup idLookup, String dbName )
        {
            var originalSecurityContext = inner.authorize( idLookup, dbName );
            var restrictedAccessMode =
                    new RestrictedAccessMode( originalSecurityContext.mode(), org.neo4j.internal.kernel.api.security.AccessMode.Static.ACCESS );
            return new EnterpriseSecurityContext( inner.subject(), restrictedAccessMode, inner.roles(), action -> false );
        }
    }
}

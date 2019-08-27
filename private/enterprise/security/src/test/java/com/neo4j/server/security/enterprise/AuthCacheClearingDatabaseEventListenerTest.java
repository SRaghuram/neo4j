/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AuthCacheClearingDatabaseEventListenerTest
{
    private CommercialAuthManager authManager = mock( CommercialAuthManager.class );
    private AuthCacheClearingDatabaseEventListener databaseEventListener = new AuthCacheClearingDatabaseEventListener( authManager );

    @Test
    void shouldClearCacheOnStandaloneAfterCommit()
    {
        databaseEventListener.afterCommit( null, null, null );
        verify( authManager ).clearAuthCache();
    }

    @Test
    void shouldNotClearCacheOnStandaloneBeforeCommit()
    {
        databaseEventListener.beforeCommit( null, null );
        verify( authManager, never() ).clearAuthCache();
    }

    @Test
    void shouldNotClearCacheOnStandaloneRollback()
    {
        databaseEventListener.afterRollback( null, null, null );
        verify( authManager, never() ).clearAuthCache();
    }

    @Test
    void shouldClearCacheOnClusterCommit()
    {
        databaseEventListener.transactionCommitted( 17 );
        verify( authManager ).clearAuthCache();
    }

    @Test
    void shouldClearCacheOnClusterStoreCopy()
    {
        databaseEventListener.storeReplaced( 17 );
        verify( authManager ).clearAuthCache();
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;

import static org.neo4j.kernel.impl.security.User.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.server.security.systemgraph.BasicSystemGraphRealm.IS_SUSPENDED;

@SuppressWarnings( "WeakerAccess" )
public class AuthProceduresBase
{
    @Context
    public EnterpriseSecurityContext securityContext;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SecurityLog securityLog;

    @Context
    public EnterpriseUserManager userManager;

    // ----------------- helpers ---------------------

    protected void kickoutUser( String username, String reason )
    {
        try
        {
            terminateTransactionsForValidUser( username );
            terminateConnectionsForValidUser( username );
        }
        catch ( Exception e )
        {
            securityLog.error( securityContext.subject(), "failed to terminate running transaction and bolt connections for " +
                    "user `%s` following %s: %s", username, reason, e.getMessage() );
            throw e;
        }
    }

    protected void terminateTransactionsForValidUser( String username )
    {
        KernelTransaction currentTx = getCurrentTx();
        getActiveTransactions()
                .stream()
                .filter( tx ->
                     tx.subject().hasUsername( username ) &&
                    !tx.isUnderlyingTransaction( currentTx )
                ).forEach( tx -> tx.markForTermination( Status.Transaction.Terminated ) );
    }

    protected void terminateConnectionsForValidUser( String username )
    {
        NetworkConnectionTracker connectionTracker = graph.getDependencyResolver().resolveDependency( NetworkConnectionTracker.class );
        connectionTracker.activeConnections()
                .stream()
                .filter( connection -> Objects.equals( username, connection.username() ) )
                .forEach( TrackedNetworkConnection::close );
    }

    private Set<KernelTransactionHandle> getActiveTransactions()
    {
        return graph.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    private KernelTransaction getCurrentTx()
    {
        return graph.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( true, graph.databaseId() );
    }

    public static class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
        }
    }

    protected UserResult userResultForSubject()
    {
        String username = securityContext.subject().username();
        return new UserResult( username, securityContext.roles(), false, false );
    }

    public static class UserResult
    {
        public final String username;
        public final List<String> roles;
        public final List<String> flags;

        UserResult( String username, Collection<String> roles, boolean changeRequired, boolean suspended )
        {
            this.username = username;
            this.roles = new ArrayList<>();
            this.roles.addAll( roles );
            this.flags = new ArrayList<>();
            if ( changeRequired )
            {
                flags.add( PASSWORD_CHANGE_REQUIRED );
            }
            if ( suspended )
            {
                flags.add( IS_SUSPENDED );
            }
        }
    }

    public static class RoleResult
    {
        public final String role;
        public final List<String> users;

        RoleResult( String role, Set<String> users )
        {
            this.role = role;
            this.users = new ArrayList<>();
            this.users.addAll( users );
        }
    }
}

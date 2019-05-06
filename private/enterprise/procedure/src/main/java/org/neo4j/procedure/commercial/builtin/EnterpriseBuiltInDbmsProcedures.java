/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.commercial.builtin;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.UncaughtCheckedException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.FunctionInformation;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.function.ThrowingFunction.catchThrown;
import static org.neo4j.function.ThrowingFunction.throwIfPresent;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class EnterpriseBuiltInDbmsProcedures
{
    private static final int HARD_CHAR_LIMIT = 2048;

    @Context
    public Log log;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SecurityContext securityContext;

    @Description( "Attaches a map of data to the transaction. The data will be printed when listing queries, and " +
                  "inserted into the query log." )
    @Procedure( name = "dbms.setTXMetaData", mode = DBMS )
    public void setTXMetaData( @Name( value = "data" ) Map<String,Object> data )
    {
        securityContext.assertCredentialsNotExpired();
        int totalCharSize = data.entrySet()
                .stream()
                .mapToInt( e -> e.getKey().length() + ((e.getValue() != null) ? e.getValue().toString().length() : 0) )
                .sum();

        if ( totalCharSize >= HARD_CHAR_LIMIT )
        {
            throw new IllegalArgumentException(
                    format( "Invalid transaction meta-data, expected the total number of chars for " +
                            "keys and values to be less than %d, got %d", HARD_CHAR_LIMIT, totalCharSize ) );
        }

        getCurrentTx().setMetaData( data );
    }

    @Description( "Provides attached transaction metadata." )
    @Procedure( name = "dbms.getTXMetaData", mode = DBMS )
    public Stream<MetadataResult> getTXMetaData()
    {
        securityContext.assertCredentialsNotExpired();
        return Stream.of( getCurrentTx().getMetaData() ).map( MetadataResult::new );
    }

    private KernelTransaction getCurrentTx()
    {
        return graph.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( true );
    }

    @Description( "List all accepted network connections at this instance that are visible to the user." )
    @Procedure( name = "dbms.listConnections", mode = DBMS )
    public Stream<ListConnectionResult> listConnections()
    {
        securityContext.assertCredentialsNotExpired();

        NetworkConnectionTracker connectionTracker = getConnectionTracker();
        ZoneId timeZone = getConfiguredTimeZone();

        return connectionTracker.activeConnections()
                .stream()
                .filter( connection -> isAdminOrSelf( connection.username() ) )
                .map( connection -> new ListConnectionResult( connection, timeZone ) );
    }

    @Description( "Kill network connection with the given connection id." )
    @Procedure( name = "dbms.killConnection", mode = DBMS )
    public Stream<ConnectionTerminationResult> killConnection( @Name( "id" ) String id )
    {
        return killConnections( singletonList( id ) );
    }

    @Description( "Kill all network connections with the given connection ids." )
    @Procedure( name = "dbms.killConnections", mode = DBMS )
    public Stream<ConnectionTerminationResult> killConnections( @Name( "ids" ) List<String> ids )
    {
        securityContext.assertCredentialsNotExpired();

        NetworkConnectionTracker connectionTracker = getConnectionTracker();

        return ids.stream().map( id -> killConnection( id, connectionTracker ) );
    }

    private NetworkConnectionTracker getConnectionTracker()
    {
        return graph.getDependencyResolver().resolveDependency( NetworkConnectionTracker.class );
    }

    private ConnectionTerminationResult killConnection( String id, NetworkConnectionTracker connectionTracker )
    {
        TrackedNetworkConnection connection = connectionTracker.get( id );
        if ( connection != null )
        {
            if ( isAdminOrSelf( connection.username() ) )
            {
                connection.close();
                return new ConnectionTerminationResult( id, connection.username() );
            }
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        return new ConnectionTerminationFailedResult( id );
    }

    @Description( "List all user functions in the DBMS." )
    @Procedure( name = "dbms.functions", mode = DBMS )
    public Stream<FunctionResult> listFunctions()
    {
        securityContext.assertCredentialsNotExpired();
        DependencyResolver resolver = graph.getDependencyResolver();
        QueryExecutionEngine queryExecutionEngine = resolver.resolveDependency( QueryExecutionEngine.class );
        List<FunctionInformation> providedLanguageFunctions = queryExecutionEngine.getProvidedLanguageFunctions();

        // gets you all functions provided by the query language
        Stream<FunctionResult> languageFunctions =
                providedLanguageFunctions.stream().map( FunctionResult::new );

        // gets you all non-aggregating functions that are registered in the db (incl. those from libs like apoc)
        Stream<FunctionResult> loadedFunctions = resolver.resolveDependency( GlobalProcedures.class ).getAllNonAggregatingFunctions()
                .map( f -> new FunctionResult( f, false ) );

        // gets you all aggregation functions that are registered in the db (incl. those from libs like apoc)
        Stream<FunctionResult> loadedAggregationFunctions = resolver.resolveDependency( GlobalProcedures.class ).getAllAggregatingFunctions()
                .map( f -> new FunctionResult( f, true ) );

        return Stream.concat( Stream.concat( languageFunctions, loadedFunctions ), loadedAggregationFunctions )
                .sorted( Comparator.comparing( a -> a.name ) );
    }

    public static class FunctionResult
    {
        public final String name;
        public final String signature;
        public final String description;
        public final boolean aggregating;
        public final List<String> roles;

        private FunctionResult( UserFunctionSignature signature, boolean isAggregation )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            roles = Stream.of( "admin", "reader", "editor", "publisher", "architect" ).collect( toList() );
            roles.addAll( Arrays.asList( signature.allowed() ) );
            this.aggregating = isAggregation;
        }

        private FunctionResult( FunctionInformation info )
        {
            this.name = info.getFunctionName();
            this.signature = info.getSignature();
            this.description = info.getDescription();
            this.aggregating = info.isAggregationFunction();
            roles = Stream.of( "admin", "reader", "editor", "publisher", "architect" ).collect( toList() );
        }
    }

    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        securityContext.assertCredentialsNotExpired();
        GlobalProcedures globalProcedures = graph.getDependencyResolver().resolveDependency( GlobalProcedures.class );
        return globalProcedures.getAllProcedures().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class ProcedureResult
    {
        // These two procedures are admin procedures but may be executed for your own user,
        // this is not documented anywhere but we cannot change the behaviour in a point release
        private static final List<String> ADMIN_PROCEDURES =
                Arrays.asList( "changeUserPassword", "listRolesForUser" );

        public final String name;
        public final String signature;
        public final String description;
        public final List<String> roles;
        public final String mode;

        public ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            this.mode = signature.mode().toString();
            roles = new ArrayList<>();
            switch ( signature.mode() )
            {
            case DBMS:
                if ( signature.admin() || isAdminProcedure( signature.name().name() ) )
                {
                    roles.add( "admin" );
                }
                else
                {
                    roles.add( "reader" );
                    roles.add( "editor" );
                    roles.add( "publisher" );
                    roles.add( "architect" );
                    roles.add( "admin" );
                    roles.addAll( Arrays.asList( signature.allowed() ) );
                }
                break;
            case DEFAULT:
            case READ:
                roles.add( "reader" );
            case WRITE:
                roles.add( "editor" );
                roles.add( "publisher" );
            case SCHEMA:
                roles.add( "architect" );
            default:
                roles.add( "admin" );
                roles.addAll( Arrays.asList( signature.allowed() ) );
            }
        }

        private boolean isAdminProcedure( String procedureName )
        {
            return name.startsWith( "dbms.security." ) && ADMIN_PROCEDURES.contains( procedureName );
        }
    }

    @Admin
    @Description( "Updates a given setting value. Passing an empty value will result in removing the configured value " +
            "and falling back to the default value. Changes will not persist and will be lost if the server is restarted." )
    @Procedure( name = "dbms.setConfigValue", mode = DBMS )
    public void setConfigValue( @Name( "setting" ) String setting, @Name( "value" ) String value )
    {
        Config config = resolver.resolveDependency( Config.class );
        config.updateDynamicSetting( setting, value, "dbms.setConfigValue" ); // throws if something goes wrong
    }

    /*
    ==================================================================================
     */

    @Description( "List all queries currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        EmbeddedProxySPI nodeManager = resolver.resolveDependency( EmbeddedProxySPI.class );
        ZoneId zoneId = getConfiguredTimeZone();
        try
        {
            return getKernelTransactions().activeTransactions().stream()
                .flatMap( KernelTransactionHandle::executingQueries )
                    .filter( query -> isAdminOrSelf( query.username() ) )
                    .map( catchThrown( InvalidArgumentsException.class,
                            query -> new QueryStatusResult( query, nodeManager, zoneId ) ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "List all transactions currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionStatusResult> listTransactions() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {
            Set<KernelTransactionHandle> handles = getKernelTransactions().activeTransactions().stream()
                    .filter( transaction -> isAdminOrSelf( transaction.subject().username() ) )
                    .collect( toSet() );

            Map<KernelTransactionHandle,List<QuerySnapshot>> handleQuerySnapshotsMap = handles.stream()
                    .collect( toMap( identity(), getTransactionQueries() ) );

            TransactionDependenciesResolver transactionBlockerResolvers =
                    new TransactionDependenciesResolver( handleQuerySnapshotsMap );

            ZoneId zoneId = getConfiguredTimeZone();

            return handles.stream()
                    .map( catchThrown( InvalidArgumentsException.class,
                            tx -> new TransactionStatusResult( tx, transactionBlockerResolvers,
                                    handleQuerySnapshotsMap, zoneId ) ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Kill transaction with provided id." )
    @Procedure( name = "dbms.killTransaction", mode = DBMS )
    public Stream<TransactionMarkForTerminationResult> killTransaction( @Name( "id" ) String transactionId )
    {
        requireNonNull( transactionId );
        return killTransactions( singletonList( transactionId ) );
    }

    @Description( "Kill transactions with provided ids." )
    @Procedure( name = "dbms.killTransactions", mode = DBMS )
    public Stream<TransactionMarkForTerminationResult> killTransactions( @Name( "ids" ) List<String> transactionIds )
    {
        requireNonNull( transactionIds );
        securityContext.assertCredentialsNotExpired();
        log.warn( "User %s trying to kill transactions: %s.", securityContext.subject().username(), transactionIds.toString() );
        Map<String,KernelTransactionHandle> handles = getKernelTransactions().activeTransactions().stream()
                        .filter( transaction -> isAdminOrSelf( transaction.subject().username() ) )
                        .filter( transaction -> transactionIds.contains( transaction.getUserTransactionName() ) )
                        .collect( toMap( KernelTransactionHandle::getUserTransactionName, identity() ) );
        return transactionIds.stream().map( id -> terminateTransaction( handles, id ) );
    }

    private TransactionMarkForTerminationResult terminateTransaction( Map<String,KernelTransactionHandle> handles, String transactionId )
    {
        KernelTransactionHandle handle = handles.get( transactionId );
        String currentUser = securityContext.subject().username();
        if ( handle == null )
        {
            return new TransactionMarkForTerminationFailedResult( transactionId, currentUser );
        }
        log.debug( "User %s terminated transaction %d.", currentUser, transactionId );
        handle.markForTermination( Status.Transaction.Terminated );
        return new TransactionMarkForTerminationResult( transactionId, handle.subject().username() );
    }

    private static Function<KernelTransactionHandle,List<QuerySnapshot>> getTransactionQueries()
    {
        return transactionHandle -> transactionHandle.executingQueries()
                                              .map( ExecutingQuery::snapshot )
                                              .collect( toList() );
    }

    @Description( "List the active lock requests granted for the transaction executing the query with the given query id." )
    @Procedure( name = "dbms.listActiveLocks", mode = DBMS )
    public Stream<ActiveLocksResult> listActiveLocks( @Name( "queryId" ) String queryId )
            throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {
            long id = QueryId.fromExternalString( queryId ).kernelQueryId();
            return getActiveTransactions( tx -> executingQueriesWithId( id, tx ) )
                    .flatMap( this::getActiveLocksForQuery );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Kill all transactions executing the query with the given query id." )
    @Procedure( name = "dbms.killQuery", mode = DBMS )
    public Stream<QueryTerminationResult> killQuery( @Name( "id" ) String idText ) throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {
            long queryId = QueryId.fromExternalString( idText ).kernelQueryId();

            Set<Pair<KernelTransactionHandle,ExecutingQuery>> querys = getActiveTransactions( tx -> executingQueriesWithId( queryId, tx ) ).collect( toSet() );
            boolean killQueryVerbose = resolver.resolveDependency( Config.class ).get( GraphDatabaseSettings.kill_query_verbose );
            if ( killQueryVerbose && querys.isEmpty() )
            {
                return Stream.<QueryTerminationResult>builder().add( new QueryFailedTerminationResult( QueryId.fromExternalString( idText ) ) ).build();
            }
            return querys.stream().map( catchThrown( InvalidArgumentsException.class, this::killQueryTransaction ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Kill all transactions executing a query with any of the given query ids." )
    @Procedure( name = "dbms.killQueries", mode = DBMS )
    public Stream<QueryTerminationResult> killQueries( @Name( "ids" ) List<String> idTexts ) throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {

            Set<Long> queryIds = idTexts.stream().map( catchThrown( InvalidArgumentsException.class, QueryId::fromExternalString ) ).map(
                    catchThrown( InvalidArgumentsException.class, QueryId::kernelQueryId ) ).collect( toSet() );

            Set<QueryTerminationResult> terminatedQuerys = getActiveTransactions( tx -> executingQueriesWithIds( queryIds, tx ) ).map(
                    catchThrown( InvalidArgumentsException.class, this::killQueryTransaction ) ).collect( toSet() );
            boolean killQueryVerbose = resolver.resolveDependency( Config.class ).get( GraphDatabaseSettings.kill_query_verbose );
            if ( killQueryVerbose && terminatedQuerys.size() != idTexts.size() )
            {
                for ( String id : idTexts )
                {
                    if ( terminatedQuerys.stream().noneMatch( query -> query.queryId.equals( id ) ) )
                    {
                        terminatedQuerys.add( new QueryFailedTerminationResult( QueryId.fromExternalString( id ) ) );
                    }
                }
            }
            return terminatedQuerys.stream();
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Initiate and wait for a new check point, or wait any already on-going check point to complete. Note that this temporarily disables the " +
            "`dbms.checkpoint.iops.limit` setting in order to make the check point complete faster. This might cause transaction throughput to degrade " +
            "slightly, due to increased IO load." )
    @Procedure( name = "dbms.checkpoint", mode = DBMS )
    public Stream<CheckpointResult> checkpoint() throws IOException
    {
        KernelTransaction kernelTransaction = getCurrentTx();
        CheckPointer checkPointer = resolver.resolveDependency( CheckPointer.class );
        // Use isTerminated as a timeout predicate to ensure that we stop waiting, if the transaction is terminated.
        BooleanSupplier timeoutPredicate = kernelTransaction::isTerminated;
        long transactionId = checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Call to dbms.checkpoint() procedure" ), timeoutPredicate );
        return Stream.of( transactionId == -1 ? CheckpointResult.TERMINATED : CheckpointResult.SUCCESS );
    }

    public enum CheckpointResult
    {
        SUCCESS( true, "Checkpoint completed." ),
        TERMINATED( false, "Transaction terminated while waiting for the requested checkpoint operation to finish." );

        public final boolean success;
        public final String message;

        CheckpointResult( boolean success, String message )
        {
            this.success = success;
            this.message = message;
        }
    }

    private <T> Stream<Pair<KernelTransactionHandle, T>> getActiveTransactions(
            Function<KernelTransactionHandle,Stream<T>> selector
    )
    {
        return getActiveTransactions( graph.getDependencyResolver() )
            .stream()
            .flatMap( tx -> selector.apply( tx ).map( data -> Pair.of( tx, data ) ) );
    }

    private static Stream<ExecutingQuery> executingQueriesWithIds( Set<Long> ids, KernelTransactionHandle txHandle )
    {
        return txHandle.executingQueries().filter( q -> ids.contains( q.internalQueryId() ) );
    }

    private static Stream<ExecutingQuery> executingQueriesWithId( long id, KernelTransactionHandle txHandle )
    {
        return txHandle.executingQueries().filter( q -> q.internalQueryId() == id );
    }

    private QueryTerminationResult killQueryTransaction( Pair<KernelTransactionHandle, ExecutingQuery> pair )
            throws InvalidArgumentsException
    {
        ExecutingQuery query = pair.other();
        if ( isAdminOrSelf( query.username() ) )
        {
            pair.first().markForTermination( Status.Transaction.Terminated );
            return new QueryTerminationResult( QueryId.ofInternalId( query.internalQueryId() ), query.username() );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private Stream<ActiveLocksResult> getActiveLocksForQuery( Pair<KernelTransactionHandle, ExecutingQuery> pair )
    {
        ExecutingQuery query = pair.other();
        if ( isAdminOrSelf( query.username() ) )
        {
            return pair.first().activeLocks().map( ActiveLocksResult::new );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private KernelTransactions getKernelTransactions()
    {
        return resolver.resolveDependency( KernelTransactions.class );
    }

    // ----------------- helpers ---------------------

    public static Stream<TransactionTerminationResult> terminateTransactionsForValidUser(
            DependencyResolver dependencyResolver, String username, KernelTransaction currentTx )
    {
        long terminatedCount = getActiveTransactions( dependencyResolver )
            .stream()
            .filter( tx -> tx.subject().hasUsername( username ) &&
                            !tx.isUnderlyingTransaction( currentTx ) )
            .map( tx -> tx.markForTermination( Status.Transaction.Terminated ) )
            .filter( marked -> marked )
            .count();
        return Stream.of( new TransactionTerminationResult( username, terminatedCount ) );
    }

    public static Set<KernelTransactionHandle> getActiveTransactions( DependencyResolver dependencyResolver )
    {
        return dependencyResolver.resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    public static Stream<TransactionResult> countTransactionByUsername( Stream<String> usernames )
    {
        return usernames
            .collect( Collectors.groupingBy( identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new TransactionResult( entry.getKey(), entry.getValue() )
        );
    }

    private ZoneId getConfiguredTimeZone()
    {
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
    }

    private boolean isAdminOrSelf( String username )
    {
        return securityContext.isAdmin() || securityContext.subject().hasUsername( username );
    }

    private void assertAdminOrSelf( String username )
    {
        if ( !isAdminOrSelf( username ) )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    public static class QueryTerminationResult
    {
        public final String queryId;
        public final String username;
        public String message = "Query found";

        public QueryTerminationResult( QueryId queryId, String username )
        {
            this.queryId = queryId.toString();
            this.username = username;
        }
    }

    public static class QueryFailedTerminationResult extends QueryTerminationResult
    {
        public QueryFailedTerminationResult( QueryId queryId )
        {
            super( queryId, "n/a" );
            super.message = "No Query found with this id";
        }
    }

    public static class TransactionResult
    {
        public final String username;
        public final Long activeTransactions;

        TransactionResult( String username, Long activeTransactions )
        {
            this.username = username;
            this.activeTransactions = activeTransactions;
        }
    }

    public static class TransactionTerminationResult
    {
        public final String username;
        public final Long transactionsTerminated;

        TransactionTerminationResult( String username, Long transactionsTerminated )
        {
            this.username = username;
            this.transactionsTerminated = transactionsTerminated;
        }
    }

    public static class MetadataResult
    {
        public final Map<String,Object> metadata;

        MetadataResult( Map<String,Object> metadata )
        {
            this.metadata = metadata;
        }
    }
}

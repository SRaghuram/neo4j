/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.FunctionInformation;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.NamedMemoryPool;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.resources.Profiler;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class EnterpriseBuiltInDbmsProcedures
{
    @Context
    public Log log;

    @Context
    public DependencyResolver resolver;

    @Context
    public Transaction transaction;

    @Context
    public SecurityContext securityContext;

    @Context
    public KernelTransaction kernelTransaction;

    @SystemProcedure
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

    @SystemProcedure
    @Description( "Kill network connection with the given connection id." )
    @Procedure( name = "dbms.killConnection", mode = DBMS )
    public Stream<ConnectionTerminationResult> killConnection( @Name( "id" ) String id )
    {
        return killConnections( singletonList( id ) );
    }

    @SystemProcedure
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
        return resolver.resolveDependency( NetworkConnectionTracker.class );
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

    @SystemProcedure
    @Description( "List all functions in the DBMS." )
    @Procedure( name = "dbms.functions", mode = DBMS )
    public Stream<FunctionResult> listFunctions()
    {
        securityContext.assertCredentialsNotExpired();
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
        public final List<String> defaultBuiltInRoles;

        private FunctionResult( UserFunctionSignature signature, boolean isAggregation )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            defaultBuiltInRoles = Stream.of( "admin", "reader", "editor", "publisher", "architect" ).collect( toList() );
            defaultBuiltInRoles.addAll( Arrays.asList( signature.allowed() ) );
            this.aggregating = isAggregation;
        }

        private FunctionResult( FunctionInformation info )
        {
            this.name = info.getFunctionName();
            this.signature = info.getSignature();
            this.description = info.getDescription();
            this.aggregating = info.isAggregationFunction();
            defaultBuiltInRoles = Stream.of( "admin", "reader", "editor", "publisher", "architect" ).collect( toList() );
        }
    }

    @SystemProcedure
    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        securityContext.assertCredentialsNotExpired();
        GlobalProcedures globalProcedures = resolver.resolveDependency( GlobalProcedures.class );
        return globalProcedures.getAllProcedures().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class ProcedureResult
    {
        // These procedures have WRITE mode but an editor is not allowed to execute them. So we need to not add that role to the list of roles
        private static final List<String> NON_EDITOR_PROCEDURES =
                Arrays.asList( "createLabel", "createProperty", "createRelationshipType" );

        public final String name;
        public final String signature;
        public final String description;
        public final String mode;
        public final List<String> defaultBuiltInRoles;
        public final boolean worksOnSystem;

        public ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            this.mode = signature.mode().toString();
            this.worksOnSystem = signature.systemProcedure();
            defaultBuiltInRoles = new ArrayList<>();
            if ( !isInvalidProcedure() )
            {
                if ( signature.admin() || isAdminProcedure() )
                {
                    defaultBuiltInRoles.add( "admin" );
                }
                else
                {
                    switch ( signature.mode() )
                    {
                    case SCHEMA:
                        defaultBuiltInRoles.add( "architect" );
                        break;
                    case WRITE:
                        if ( !NON_EDITOR_PROCEDURES.contains( signature.name().name() ) )
                        {
                            defaultBuiltInRoles.add( "editor" );
                        }
                        defaultBuiltInRoles.add( "publisher" );
                        defaultBuiltInRoles.add( "architect" );
                        break;
                    default:
                        defaultBuiltInRoles.add( "reader" );
                        defaultBuiltInRoles.add( "editor" );
                        defaultBuiltInRoles.add( "publisher" );
                        defaultBuiltInRoles.add( "architect" );
                    }
                    defaultBuiltInRoles.add( "admin" );
                    defaultBuiltInRoles.addAll( Arrays.asList( signature.allowed() ) );
                }
            }
        }

        private boolean isAdminProcedure()
        {
            // This procedure asserts admin right internally (to be able to execute for your own user) so we can't rely on the signature to detect that
            return name.startsWith( "dbms.security.listRolesForUser" );
        }

        private boolean isInvalidProcedure()
        {
            // This procedure has been disabled and always throws an error
            return name.startsWith( "dbms.security.changePassword" );
        }
    }

    @Admin
    @SystemProcedure
    @Description( "Updates a given setting value. Passing an empty value will result in removing the configured value " +
            "and falling back to the default value. Changes will not persist and will be lost if the server is restarted." )
    @Procedure( name = "dbms.setConfigValue", mode = DBMS )
    public void setConfigValue( @Name( "setting" ) String setting, @Name( "value" ) String value )
    {
        Config config = resolver.resolveDependency( Config.class );
        SettingImpl<Object> settingObj = (SettingImpl<Object>) config.getSetting( setting );
        SettingsWhitelist settingsWhiteList = resolver.resolveDependency( SettingsWhitelist.class );
        if ( settingsWhiteList.isWhiteListed( setting ) )
        {
            config.setDynamic( settingObj, settingObj.parse( isNotEmpty( value ) ? value : null ), "dbms.setConfigValue" );
        }
        else
        {
            throw new AuthorizationViolationException( "Failed to set value for `" + setting + "` using procedure `dbms.setConfigValue`: access denied." );
        }
    }

    /*
    ==================================================================================
     */

    @SystemProcedure
    @Description( "List all queries currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        ZoneId zoneId = getConfiguredTimeZone();
        List<QueryStatusResult> result = new ArrayList<>();
        for ( DatabaseContext databaseContext : getDatabaseManager().registeredDatabases().values() )
        {
            DatabaseScope dbScope = new DatabaseScope( databaseContext.database().getNamedDatabaseId().name() );
            for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
            {
                if ( tx.executingQuery().isPresent() )
                {
                    ExecutingQuery query = tx.executingQuery().get();
                    String username = query.username();
                    var action = new AdminActionOnResource( PrivilegeAction.SHOW_TRANSACTION, dbScope, new UserSegment( username ) );
                    if ( isSelfOrAllows( username, action ) )
                    {
                        result.add(
                                new QueryStatusResult( query, (InternalTransaction) transaction, zoneId, databaseContext.databaseFacade().databaseName() ) );
                    }
                }
            }
        }
        return result.stream();
    }

    @SystemProcedure
    @Description( "List all memory pools currently registered at this instance that are visible to the user." )
    @Procedure( name = "dbms.listPools", mode = DBMS )
    public Stream<MemoryPoolResult> listMemoryPools()
    {
        var memoryPools = resolver.resolveDependency( MemoryPools.class );
        var registeredPools = memoryPools.getPools();
        registeredPools.sort( Comparator.comparing( NamedMemoryPool::group )
                .thenComparing( NamedMemoryPool::name ) );
        return registeredPools.stream().map( MemoryPoolResult::new );
    }

    @SystemProcedure
    @Description( "List all memory pools, including sub pools, currently registered at this instance that are visible to the user." )
    @Procedure( name = "dbms.listPoolsExt", mode = DBMS )
    public Stream<MemoryPoolResult> listMemoryPoolsExt()
    {
        var memoryPools = resolver.resolveDependency( MemoryPools.class );
        var registeredPools = memoryPools.getPools();
        var allPools = new ArrayList<>( registeredPools );
        for ( NamedMemoryPool registeredPool : registeredPools )
        {
            allPools.addAll( registeredPool.getSubPools() );
        }
        allPools.sort( Comparator.comparing( NamedMemoryPool::group )
                .thenComparing( NamedMemoryPool::name ) );
        return allPools.stream().map( MemoryPoolResult::new );
    }

    @SystemProcedure
    @Description( "List all transactions currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionStatusResult> listTransactions() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        ZoneId zoneId = getConfiguredTimeZone();
        List<TransactionStatusResult> result = new ArrayList<>();
        for ( DatabaseContext databaseContext : getDatabaseManager().registeredDatabases().values() )
        {
            DatabaseScope dbScope = new DatabaseScope( databaseContext.database().getNamedDatabaseId().name() );
            Map<KernelTransactionHandle,Optional<QuerySnapshot>> handleQuerySnapshotsMap = new HashMap<>();
            for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
            {
                String username = tx.subject().username();
                var action = new AdminActionOnResource( PrivilegeAction.SHOW_TRANSACTION, dbScope, new UserSegment( username ) );
                if ( isSelfOrAllows( username, action ) )
                {
                    handleQuerySnapshotsMap.put( tx, tx.executingQuery().map( ExecutingQuery::snapshot ) );
                }
            }
            TransactionDependenciesResolver transactionBlockerResolvers = new TransactionDependenciesResolver( handleQuerySnapshotsMap );

            for ( KernelTransactionHandle tx : handleQuerySnapshotsMap.keySet() )
            {
                result.add( new TransactionStatusResult( databaseContext.databaseFacade().databaseName(), tx, transactionBlockerResolvers,
                        handleQuerySnapshotsMap, zoneId ) );
            }
        }

        return result.stream();
    }

    @SystemProcedure
    @Description( "Kill transaction with provided id." )
    @Procedure( name = "dbms.killTransaction", mode = DBMS )
    public Stream<TransactionMarkForTerminationResult> killTransaction( @Name( "id" ) String transactionId ) throws InvalidArgumentsException
    {
        requireNonNull( transactionId );
        return killTransactions( singletonList( transactionId ) );
    }

    @SystemProcedure
    @Description( "Kill transactions with provided ids." )
    @Procedure( name = "dbms.killTransactions", mode = DBMS )
    public Stream<TransactionMarkForTerminationResult> killTransactions( @Name( "ids" ) List<String> transactionIds ) throws InvalidArgumentsException
    {
        requireNonNull( transactionIds );
        securityContext.assertCredentialsNotExpired();
        log.warn( "User %s trying to kill transactions: %s.", securityContext.subject().username(), transactionIds.toString() );

        DatabaseManager<DatabaseContext> databaseManager = getDatabaseManager();
        DatabaseIdRepository databaseIdRepository = databaseManager.databaseIdRepository();

        Map<NamedDatabaseId,Set<DbmsTransactionId>> byDatabase = new HashMap<>();
        for ( String idText : transactionIds )
        {
            DbmsTransactionId id = new DbmsTransactionId( idText );
            Optional<NamedDatabaseId> namedDatabaseId = databaseIdRepository.getByName( id.database() );
            namedDatabaseId.ifPresent( databaseId -> byDatabase.computeIfAbsent( databaseId, ignore -> new HashSet<>() ).add( id ) );
        }

        Map<String,KernelTransactionHandle> handles = new HashMap<>( transactionIds.size() );
        for ( Map.Entry<NamedDatabaseId,Set<DbmsTransactionId>> entry : byDatabase.entrySet() )
        {
            NamedDatabaseId databaseId = entry.getKey();
            var dbScope = new DatabaseScope( databaseId.name() );
            Optional<DatabaseContext> maybeDatabaseContext = databaseManager.getDatabaseContext( databaseId );
            if ( maybeDatabaseContext.isPresent() )
            {
                Set<DbmsTransactionId> txIds = entry.getValue();
                DatabaseContext databaseContext = maybeDatabaseContext.get();
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    String username = tx.subject().username();
                    var action = new AdminActionOnResource( PrivilegeAction.TERMINATE_TRANSACTION, dbScope, new UserSegment( username ) );
                    if ( !isSelfOrAllows( username, action ) )
                    {
                        continue;
                    }
                    DbmsTransactionId txIdRepresentation = new DbmsTransactionId( databaseId.name(), tx.getUserTransactionId() );
                    if ( txIds.contains( txIdRepresentation ) )
                    {
                        handles.put( txIdRepresentation.toString(), tx );
                    }
                }
            }
        }

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
        if ( handle.isClosing() )
        {
            return new TransactionMarkForTerminationFailedResult( transactionId, currentUser, "Unable to kill closing transactions." );
        }
        log.debug( "User %s terminated transaction %d.", currentUser, transactionId );
        handle.markForTermination( Status.Transaction.Terminated );
        return new TransactionMarkForTerminationResult( transactionId, handle.subject().username() );
    }

    @SystemProcedure
    @Description( "List the active lock requests granted for the transaction executing the query with the given query id." )
    @Procedure( name = "dbms.listActiveLocks", mode = DBMS )
    public Stream<ActiveLocksResult> listActiveLocks( @Name( "queryId" ) String queryIdText )
            throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        DbmsQueryId dbmsQueryId = new DbmsQueryId( queryIdText );

        DatabaseManager<DatabaseContext> databaseManager = getDatabaseManager();
        DatabaseIdRepository databaseIdRepository = databaseManager.databaseIdRepository();
        Optional<NamedDatabaseId> maybeNamedDatabaseId = databaseIdRepository.getByName( dbmsQueryId.database() );
        if ( maybeNamedDatabaseId.isPresent() )
        {
            NamedDatabaseId namedDatabaseId = maybeNamedDatabaseId.get();
            Optional<DatabaseContext> maybeDatabaseContext = databaseManager.getDatabaseContext( namedDatabaseId );
            if ( maybeDatabaseContext.isPresent() )
            {
                DatabaseContext databaseContext = maybeDatabaseContext.get();
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    if ( tx.executingQuery().isPresent() )
                    {
                        ExecutingQuery query = tx.executingQuery().get();
                        if ( query.internalQueryId() == dbmsQueryId.internalId() )
                        {
                            if ( isAdminOrSelf( query.username() ) )
                            {
                                return tx.activeLocks().map( ActiveLocksResult::new );
                            }
                            else
                            {
                                throw new AuthorizationViolationException( PERMISSION_DENIED );
                            }
                        }
                    }
                }
            }
        }
        return Stream.empty();
    }

    @SystemProcedure
    @Description( "Kill all transactions executing the query with the given query id." )
    @Procedure( name = "dbms.killQuery", mode = DBMS )
    public Stream<QueryTerminationResult> killQuery( @Name( "id" ) String idText ) throws InvalidArgumentsException
    {
        return killQueries( singletonList( idText ) );
    }

    @SystemProcedure
    @Description( "Kill all transactions executing a query with any of the given query ids." )
    @Procedure( name = "dbms.killQueries", mode = DBMS )
    public Stream<QueryTerminationResult> killQueries( @Name( "ids" ) List<String> idTexts ) throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        DatabaseManager<DatabaseContext> databaseManager = getDatabaseManager();
        DatabaseIdRepository databaseIdRepository = databaseManager.databaseIdRepository();

        Set<NamedDatabaseId> affectedDatabases = new HashSet<>();
        Set<DbmsQueryId> dbmsQueryIds = new HashSet<>( idTexts.size() );
        for ( String idText : idTexts )
        {
            DbmsQueryId id = new DbmsQueryId( idText );
            dbmsQueryIds.add( id );
            databaseIdRepository.getByName( id.database() ).ifPresent( affectedDatabases::add );
        }

        // Kill the ones we find
        List<QueryTerminationResult> result = new ArrayList<>( dbmsQueryIds.size() );
        for ( NamedDatabaseId databaseId : affectedDatabases )
        {
            Optional<DatabaseContext> maybeDatabaseContext = databaseManager.getDatabaseContext( databaseId );
            if ( maybeDatabaseContext.isPresent() )
            {
                DatabaseContext databaseContext = maybeDatabaseContext.get();
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    DbmsQueryId internalDbmsQueryId = new DbmsQueryId(
                            databaseContext.databaseFacade().databaseName(),
                            tx.executingQuery().map( ExecutingQuery::internalQueryId ).orElse( -1L )
                    );
                    if ( dbmsQueryIds.remove( internalDbmsQueryId ) )
                    {
                        result.add( killQueryTransaction( internalDbmsQueryId, tx, databaseId ) );
                    }
                }
            }
        }

        // Add error about the rest
        for ( DbmsQueryId dbmsQueryId : dbmsQueryIds )
        {
            result.add( new QueryFailedTerminationResult( dbmsQueryId, "n/a", "No Query found with this id" ) );
        }

        return result.stream();
    }

    @Admin
    @SystemProcedure
    @Description( "List the job groups that are active in the database internal job scheduler." )
    @Procedure( name = "dbms.scheduler.groups", mode = DBMS )
    public Stream<ActiveSchedulingGroup> schedulerActiveGroups()
    {
        JobScheduler scheduler = resolver.resolveDependency( JobScheduler.class );
        return scheduler.activeGroups().map( ActiveSchedulingGroup::new );
    }

    @Admin
    @SystemProcedure
    @Description( "Begin profiling all threads within the given job group, for the specified duration. " +
            "Note that profiling incurs overhead to a system, and will slow it down." )
    @Procedure( name = "dbms.scheduler.profile", mode = DBMS )
    public Stream<ProfileResult> schedulerProfileGroup(
            @Name( "method" ) String method,
            @Name( "group" ) String groupName,
            @Name( "duration" ) String duration ) throws InterruptedException
    {
        Profiler profiler;
        if ( "sample".equals( method ) )
        {
            profiler = Profiler.profiler();
        }
        else
        {
            throw new IllegalArgumentException( "No such profiling method: '" + method + "'. Valid methods are: 'sample'." );
        }
        Group group = null;
        for ( Group value : Group.values() )
        {
            if ( value.groupName().equals( groupName ) )
            {
                group = value;
                break;
            }
        }
        if ( group == null )
        {
            throw new IllegalArgumentException( "No such scheduling group: '" + groupName + "'." );
        }
        long durationNanos = TimeUnit.MILLISECONDS.toNanos( TimeUtil.parseTimeMillis.apply( duration ) );
        JobScheduler scheduler = resolver.resolveDependency( JobScheduler.class );
        long deadline = System.nanoTime() + durationNanos;
        try
        {
            scheduler.profileGroup( group, profiler );
            while ( System.nanoTime() < deadline )
            {
                kernelTransaction.assertOpen();
                Thread.sleep( 100 );
            }
        }
        finally
        {
            profiler.finish();
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( baos );
        profiler.printProfile( out, "Profiled group '" + group + "'." );
        out.flush();
        return Stream.of( new ProfileResult( baos.toString() ) );
    }

    @SystemProcedure
    @Description( "Initiate and wait for a new check point, or wait any already on-going check point to complete. Note that this temporarily disables the " +
            "`dbms.checkpoint.iops.limit` setting in order to make the check point complete faster. This might cause transaction throughput to degrade " +
            "slightly, due to increased IO load." )
    @Procedure( name = "db.checkpoint", mode = DBMS )
    public Stream<CheckpointResult> checkpoint() throws IOException
    {
        CheckPointer checkPointer = resolver.resolveDependency( CheckPointer.class );
        // Use isTerminated as a timeout predicate to ensure that we stop waiting, if the transaction is terminated.
        BooleanSupplier timeoutPredicate = kernelTransaction::isTerminated;
        long transactionId = checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Call to db.checkpoint() procedure" ), timeoutPredicate );
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

    @SuppressWarnings( "unchecked" )
    private DatabaseManager<DatabaseContext> getDatabaseManager()
    {
        return (DatabaseManager<DatabaseContext>) resolver.resolveDependency( DatabaseManager.class );
    }

    private static Set<KernelTransactionHandle> getExecutingTransactions( DatabaseContext databaseContext )
    {
        return databaseContext.dependencies().resolveDependency( KernelTransactions.class ).executingTransactions();
    }

    private QueryTerminationResult killQueryTransaction( DbmsQueryId dbmsQueryId, KernelTransactionHandle handle, NamedDatabaseId databaseId )
    {
        Optional<ExecutingQuery> query = handle.executingQuery();
        ExecutingQuery executingQuery = query.orElseThrow( () -> new IllegalStateException( "Query should exist since we filtered based on query ids" ) );
        String username = executingQuery.username();
        var action = new AdminActionOnResource( PrivilegeAction.TERMINATE_TRANSACTION, new DatabaseScope( databaseId.name() ), new UserSegment( username ) );
        if ( isSelfOrAllows( username, action ) )
        {
            if ( handle.isClosing() )
            {
                return new QueryFailedTerminationResult( dbmsQueryId, username, "Unable to kill queries when underlying transaction is closing." );
            }
            handle.markForTermination( Status.Transaction.Terminated );
            return new QueryTerminationResult( dbmsQueryId, username, "Query found" );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private ZoneId getConfiguredTimeZone()
    {
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
    }

    private boolean isSelfOrAllows( String username, AdminActionOnResource actionOnResource )
    {
        return securityContext.subject().hasUsername( username ) || securityContext.allowsAdminAction( actionOnResource );
    }

    private boolean isAdminOrSelf( String username )
    {
        return securityContext.isAdmin() || securityContext.subject().hasUsername( username );
    }

    public static class QueryTerminationResult
    {
        public final String queryId;
        public final String username;
        public final String message;

        public QueryTerminationResult( DbmsQueryId dbmsQueryId, String username, String message )
        {
            this.queryId = dbmsQueryId.toString();
            this.username = username;
            this.message = message;
        }
    }

    public static class QueryFailedTerminationResult extends QueryTerminationResult
    {
        public QueryFailedTerminationResult( DbmsQueryId dbmsQueryId, String username, String message )
        {
            super( dbmsQueryId, username, message );
        }
    }

    public static class ActiveSchedulingGroup
    {
        public final String group;
        public final long threads;

        ActiveSchedulingGroup( ActiveGroup activeGroup )
        {
            this.group = activeGroup.group.groupName();
            this.threads = activeGroup.threads;
        }
    }

    public static class ProfileResult
    {
        public final String profile;

        public ProfileResult( String profile )
        {
            this.profile = profile;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class MemoryPoolResult
    {
        private static final String UNBOUNDED = "Unbounded";
        public final String group;
        public final String poolName;
        public final String heapMemoryUsed;
        public final String heapMemoryUsedBytes;
        public final String nativeMemoryUsed;
        public final String nativeMemoryUsedBytes;
        public final String freeMemory;
        public final String freeMemoryBytes;
        public final String totalPoolMemory;
        public final String totalPoolMemoryBytes;

        public MemoryPoolResult( NamedMemoryPool memoryPool )
        {
            this.group = memoryPool.group().getName();
            this.poolName = memoryPool.name();
            this.heapMemoryUsed = bytesToString( memoryPool.usedHeap() );
            this.heapMemoryUsedBytes = valueOf( memoryPool.usedHeap() );
            this.nativeMemoryUsed = bytesToString( memoryPool.usedNative() );
            this.nativeMemoryUsedBytes = valueOf( memoryPool.usedNative() );
            if ( memoryPool.totalSize() != Long.MAX_VALUE )
            {
                this.freeMemory = bytesToString( memoryPool.free() );
                this.freeMemoryBytes = valueOf( memoryPool.free() );
                this.totalPoolMemory = bytesToString( memoryPool.totalSize() );
                this.totalPoolMemoryBytes = valueOf( memoryPool.totalSize() );
            }
            else
            {
                this.freeMemory = UNBOUNDED;
                this.freeMemoryBytes = UNBOUNDED;
                this.totalPoolMemory = UNBOUNDED;
                this.totalPoolMemoryBytes = UNBOUNDED;
            }
        }
    }
}

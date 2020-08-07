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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.query.FunctionInformation;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.builtin.QueryId;
import org.neo4j.procedure.builtin.TransactionId;
import org.neo4j.resources.Profiler;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.procedure.Mode.DBMS;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class EnterpriseBuiltInDbmsProcedures
{
    private static final String MISSING_TRANSACTION_ID = "Missing Transaction Id.";
    public static final String INCORRECT_TRANSACTION_ID_PREFIX = "Incorrect transaction id: ";

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

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SystemGraphComponents systemGraphComponents;

    @Context
    public ProcedureCallContext callContext;

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
        public final String category;
        public final String description;
        public final boolean aggregating;
        public final List<String> defaultBuiltInRoles;

        private FunctionResult( UserFunctionSignature signature, boolean isAggregation )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.category = signature.category().orElse( "" );
            this.description = signature.description().orElse( "" );
            defaultBuiltInRoles = Stream.of( "admin", "reader", "editor", "publisher", "architect" ).collect( toList() );
            defaultBuiltInRoles.addAll( Arrays.asList( signature.allowed() ) );
            this.aggregating = isAggregation;
        }

        private FunctionResult( FunctionInformation info )
        {
            this.name = info.getFunctionName();
            this.signature = info.getSignature();
            this.category = info.getCategory();
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
                .filter( proc -> !proc.internal() )
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
            return name.startsWith( "dbms.security.listRolesForUser" ) || name.startsWith( "dbms.upgrade" );
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
    @Description( "List all memory pools, including sub pools, currently registered at this instance that are visible to the user." )
    @Procedure( name = "dbms.listPools", mode = DBMS )
    public Stream<MemoryPoolResult> listMemoryPools()
    {
        var memoryPools = resolver.resolveDependency( MemoryPools.class );
        var registeredPools = memoryPools.getPools();
        List<ScopedMemoryPool> allPools = new ArrayList<>( registeredPools );
        for ( var pool : registeredPools )
        {
            allPools.addAll( pool.getDatabasePools() );
        }
        allPools.sort( Comparator.comparing( ScopedMemoryPool::group )
                .thenComparing( ScopedMemoryPool::databaseName ) );
        return allPools.stream().map( MemoryPoolResult::new );
    }

    @SystemProcedure
    @Description( "List the active lock requests granted for the transaction executing the query with the given query id." )
    @Procedure( name = "dbms.listActiveLocks", mode = DBMS )
    public Stream<ActiveLockResult> listActiveLocks( @Name( "queryId" ) String queryIdText )
            throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        QueryId queryId = QueryId.parse( queryIdText );

        DatabaseManager<DatabaseContext> databaseManager = getDatabaseManager();
        for ( Map.Entry<NamedDatabaseId,DatabaseContext> databaseEntry : databaseManager.registeredDatabases().entrySet() )
        {
            NamedDatabaseId databaseId = databaseEntry.getKey();
            DatabaseContext databaseContext = databaseEntry.getValue();
            for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
            {
                if ( tx.executingQuery().isPresent() )
                {
                    ExecutingQuery query = tx.executingQuery().get();
                    if ( query.internalQueryId() == queryId.internalId() )
                    {
                        if ( isAdminOrSelf( query.username() ) )
                        {
                            return tx.activeLocks().map( ActiveLockResult::new );
                        }
                        else
                        {
                            throw new AuthorizationViolationException(
                                    format( "Executing admin procedure is not allowed for %s.", securityContext.description() ) );
                        }
                    }
                }
            }
        }
        return Stream.empty();
    }

    @Admin
    @SystemProcedure
    @Description( "List all locks at this database." )
    @Procedure( name = "db.listLocks", mode = DBMS )
    public Stream<LockResult> listLocks()
    {
        securityContext.assertCredentialsNotExpired();

        var locks = resolver.resolveDependency( Locks.class );
        var locksList = new ArrayList<LockResult>();
        locks.accept( ( lockType, resourceType, txId, resourceId, description, estimatedWaitTime, lockIdentityHashCode ) ->
                locksList.add( new LockResult( lockType.getDescription(), resourceType.name(), resourceId, getTransactionId( txId ) ) ) );
        return locksList.stream();
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

    @Admin
    @SystemProcedure
    @Description( "List all jobs that are active in the database internal job scheduler." )
    @Procedure( name = "dbms.scheduler.jobs", mode = DBMS )
    public Stream<JobStatusResult> schedulerJobs()
    {
        JobScheduler jobScheduler = resolver.resolveDependency( JobScheduler.class );
        ZoneId zoneId = getConfiguredTimeZone();
        return jobScheduler.getMonitoredJobs().stream().map( job -> new JobStatusResult( job, zoneId ) );
    }

    @Admin
    @SystemProcedure
    @Description( "List failed job runs. There is a limit for amount of historical data." )
    @Procedure( name = "dbms.scheduler.failedJobs", mode = DBMS )
    public Stream<FailedJobRunResult> schedulerFailedJobRuns()
    {
        JobScheduler jobScheduler = resolver.resolveDependency( JobScheduler.class );
        ZoneId zoneId = getConfiguredTimeZone();
        return jobScheduler.getFailedJobRuns().stream().map( failedJobRun -> new FailedJobRunResult( failedJobRun, zoneId ) );
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

    @Admin
    @Internal
    @SystemProcedure
    @Description( "Report the current status of the system database sub-graph schema, providing details for each sub-graph component." )
    @Procedure( name = "dbms.upgradeStatusDetails", mode = READ )
    public Stream<SystemGraphComponentStatusResultDetails> upgradeStatusDetails() throws ProcedureException
    {
        assertAllowedUpgradeProc();
        if ( !callContext.isSystemDatabase() )
        {
            throw new ProcedureException( ProcedureCallFailed,
                    "This is an administration command and it should be executed against the system database: dbms.upgradeStatusDetails" );
        }
        SystemGraphComponents versions = systemGraphComponents;
        ArrayList<SystemGraphComponentStatusResultDetails> results = new ArrayList<>();
        versions.forEach( version -> results.add( new SystemGraphComponentStatusResultDetails( version.component(), version.detect( transaction ) ) ) );
        return Stream.concat( Stream.of( new SystemGraphComponentStatusResultDetails( versions.component(), versions.detect( transaction ) ) ),
                results.stream() );
    }

    @Admin
    @Internal
    @SystemProcedure
    @Description( "Upgrade the system database schema if it is not the current schema, providing upgrade status results for each sub-graph component." )
    @Procedure( name = "dbms.upgradeDetails", mode = WRITE )
    public Stream<SystemGraphComponentUpgradeResultDetails> upgradeDetails() throws ProcedureException
    {
        assertAllowedUpgradeProc();
        if ( !callContext.isSystemDatabase() )
        {
            throw new ProcedureException( ProcedureCallFailed,
                    "This is an administration command and it should be executed against the system database: dbms.upgradeDetails" );
        }
        SystemGraphComponents versions = systemGraphComponents;
        SystemGraphComponent.Status status = versions.detect( transaction );
        ArrayList<SystemGraphComponentUpgradeResultDetails> results = new ArrayList<>();
        if ( status == REQUIRES_UPGRADE )
        {
            ArrayList<SystemGraphComponent> failed = new ArrayList<>();
            versions.forEach( component ->
            {
                SystemGraphComponent.Status initialStatus = component.detect( transaction );
                if ( initialStatus == REQUIRES_UPGRADE )
                {
                    try
                    {
                        component.upgradeToCurrent( graph );
                        results.add(
                                new SystemGraphComponentUpgradeResultDetails( component.component(), component.detect( transaction ).name(), "Upgraded" ) );

                    }
                    catch ( Exception e )
                    {
                        failed.add( component );
                        results.add( new SystemGraphComponentUpgradeResultDetails( component.component(), initialStatus.name(), e.toString() ) );
                    }
                }
                else
                {
                    results.add( new SystemGraphComponentUpgradeResultDetails( component.component(), initialStatus.name(), "" ) );
                }
            } );
            String upgradeResult =
                    failed.isEmpty() ? "Success" : "Failed: " + failed.stream().map( SystemGraphComponent::component ).collect( Collectors.joining( ", " ) );
            return Stream.concat(
                    Stream.of( new SystemGraphComponentUpgradeResultDetails( versions.component(), versions.detect( transaction ).name(), upgradeResult ) ),
                    results.stream() );
        }
        else
        {
            versions.forEach(
                    version -> results.add( new SystemGraphComponentUpgradeResultDetails( version.component(), version.detect( transaction ).name(), "" ) ) );
            return Stream.concat( Stream.of( new SystemGraphComponentUpgradeResultDetails( versions.component(), versions.detect( transaction ).name(), "" ) ),
                    results.stream() );
        }
    }

    private void assertAllowedUpgradeProc()
    {
        Config config = graph.getDependencyResolver().resolveDependency( Config.class );
        if ( config.get( GraphDatabaseInternalSettings.restrict_upgrade ) )
        {
            if ( !securityContext.subject().hasUsername( config.get( GraphDatabaseInternalSettings.upgrade_username ) ) )
            {
                throw new AuthorizationViolationException(
                        String.format( "%s Execution of this procedure has been restricted by the system.", PERMISSION_DENIED ) );
            }
        }
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

    private String getTransactionId( long txId )
    {
        try
        {
            if ( txId > 0 )
            {
                return new TransactionId( graph.databaseName(), txId ).toString();
            }
        }
        catch ( InvalidArgumentsException e )
        {
            return INCORRECT_TRANSACTION_ID_PREFIX + txId;
        }
        return MISSING_TRANSACTION_ID;
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

    private ZoneId getConfiguredTimeZone()
    {
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
    }

    private boolean isAdminOrSelf( String username )
    {
        return securityContext.allowExecuteAdminProcedure( callContext.id() ) || securityContext.subject().hasUsername( username );
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

    public static class SystemGraphComponentStatusResultDetails
    {
        public final String component;
        public final String status;
        public final String description;
        public final String resolution;

        SystemGraphComponentStatusResultDetails( String component, SystemGraphComponent.Status status )
        {
            this.component = component;
            this.status = status.name();
            this.description = status.description();
            this.resolution = status.resolution();
        }
    }

    public static class SystemGraphComponentUpgradeResultDetails
    {
        public final String component;
        public final String status;
        public final String upgradeResult;

        SystemGraphComponentUpgradeResultDetails( String component, String status, String upgradeResult )
        {
            this.component = component;
            this.status = status;
            this.upgradeResult = upgradeResult;
        }
    }

    public static class LockResult
    {
        public final String mode;
        public final String resourceType;
        public final long resourceId;
        public final String transactionId;

        public LockResult( String mode, String resourceType, long resourceId, String transactionId )
        {
            this.mode = mode;
            this.resourceType = resourceType;
            this.resourceId = resourceId;
            this.transactionId = transactionId;
        }
    }

    public static class ActiveLockResult
    {
        public final String mode;
        public final String resourceType;
        public final long resourceId;

        public ActiveLockResult( ActiveLock activeLock )
        {
            this( activeLock.lockType().getDescription(), activeLock.resourceType().name(), activeLock.resourceId() );
        }

        public ActiveLockResult( String mode, String resourceType, long resourceId )
        {
            this.mode = mode;
            this.resourceType = resourceType;
            this.resourceId = resourceId;
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
        public final String pool;
        public final String databaseName;
        public final String heapMemoryUsed;
        public final String heapMemoryUsedBytes;
        public final String nativeMemoryUsed;
        public final String nativeMemoryUsedBytes;
        public final String freeMemory;
        public final String freeMemoryBytes;
        public final String totalPoolMemory;
        public final String totalPoolMemoryBytes;

        public MemoryPoolResult( ScopedMemoryPool memoryPool )
        {
            this.pool = memoryPool.group().getName();
            this.databaseName = memoryPool.databaseName();
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

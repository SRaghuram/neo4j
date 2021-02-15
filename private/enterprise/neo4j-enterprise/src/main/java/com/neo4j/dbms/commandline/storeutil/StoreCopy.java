/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.SchemaStorage;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStorage35;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore35;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.SchemaStatementProcedure;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

import static java.lang.String.format;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.internal.recordstorage.StoreTokens.allReadableTokens;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.ignore;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.findLatestFormatInFamily;
import static org.neo4j.kernel.impl.store.format.RecordStorageCapability.FLEXIBLE_SCHEMA_STORE;
import static org.neo4j.kernel.impl.storemigration.IndexConfigMigrator.migrateIndexConfig;
import static org.neo4j.kernel.impl.storemigration.IndexProviderMigrator.upgradeIndexProvider;
import static org.neo4j.logging.Level.DEBUG;
import static org.neo4j.logging.Level.INFO;

public class StoreCopy
{
    private static final String STORE_COPY_TAG = "storeCopy";
    private final DatabaseLayout from;
    private final Config config;
    private final boolean verbose;
    private final PageCacheTracer pageCacheTracer;
    private final FormatEnum format;
    private final List<String> deleteNodesWithLabels;
    private final List<String> keepOnlyNodesWithLabels;
    private final List<String> skipLabels;
    private final List<String> skipProperties;
    private final List<List<String>> skipNodeProperties;
    private final List<List<String>> keepOnlyNodeProperties;
    private final List<List<String>> skipRelationshipProperties;
    private final List<List<String>> keepOnlyRelationshipProperties;
    private final List<String> skipRelationships;
    private final PrintStream out;
    private final SystemNanoClock clock;

    private StoreCopyFilter storeCopyFilter;
    private MutableMap<String,List<NamedToken>> recreatedTokens;
    private TokenHolders tokenHolders;
    private NodeStore nodeStore;
    private PropertyStore propertyStore;
    private RelationshipStore relationshipStore;
    private StoreCopyStats stats;

    public enum FormatEnum
    {
        same,
        standard,
        high_limit,
        aligned
    }

    public StoreCopy( DatabaseLayout from, Config config, FormatEnum format,
                      List<String> deleteNodesWithLabels, List<String> keepOnlyNodesWithLabels, List<String> skipLabels,
                      List<String> skipProperties, List<List<String>> skipNodeProperties, List<List<String>> keepOnlyNodeProperties,
                      List<List<String>> skipRelationshipProperties, List<List<String>> keepOnlyRelationshipProperties,
                      List<String> skipRelationships, boolean verbose, PrintStream out,
                      PageCacheTracer pageCacheTracer, SystemNanoClock clock )
    {
        this.from = from;
        this.config = config;
        this.format = format;
        this.deleteNodesWithLabels = deleteNodesWithLabels;
        this.keepOnlyNodesWithLabels = keepOnlyNodesWithLabels;
        this.skipLabels = skipLabels;
        this.skipProperties = skipProperties;
        this.skipNodeProperties = skipNodeProperties;
        this.keepOnlyNodeProperties = keepOnlyNodeProperties;
        this.skipRelationshipProperties = skipRelationshipProperties;
        this.keepOnlyRelationshipProperties = keepOnlyRelationshipProperties;
        this.skipRelationships = skipRelationships;
        this.out = out;
        this.verbose = verbose;
        this.pageCacheTracer = pageCacheTracer;
        this.clock = clock;
    }

    public void copyTo( DatabaseLayout toDatabaseLayout, String fromPageCacheMemory, String toPageCacheMemory ) throws Exception
    {
        Path logFilePath = getLogFilePath( config );
        try ( OutputStream logFile = new BufferedOutputStream( Files.newOutputStream( logFilePath ) );
              Log4jLogProvider log1 = getLog( logFile );
              Log4jLogProvider log2 = getLog( out ) )
        {
            LogProvider logProvider = new DuplicatingLogProvider( log1, log2 );
            Log log = logProvider.getLog( "StoreCopy" );
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_COPY_TAG );
                  FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                  JobScheduler scheduler = createInitialisedScheduler();
                  PageCache fromPageCache = createPageCache( fs, fromPageCacheMemory, scheduler, clock, config );
                  NeoStores neoStores = new StoreFactory( from, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), fromPageCache, fs,
                                                          NullLogProvider.getInstance(), pageCacheTracer ).openAllNeoStores() )
            {
                out.println( "Starting to copy store, output will be saved to: " + logFilePath.toAbsolutePath() );
                nodeStore = neoStores.getNodeStore();
                propertyStore = neoStores.getPropertyStore();
                relationshipStore = neoStores.getRelationshipStore();
                recreatedTokens = Maps.mutable.empty();
                stats = new StoreCopyStats( log );
                tokenHolders = createTokenHolders( neoStores, cursorTracer );

                storeCopyFilter = convertFilter( deleteNodesWithLabels, keepOnlyNodesWithLabels, skipLabels, skipProperties, skipNodeProperties,
                                                 keepOnlyNodeProperties, skipRelationshipProperties, keepOnlyRelationshipProperties, skipRelationships,
                                                 tokenHolders, stats );

                RecordFormats recordFormats = setupRecordFormats( neoStores.getRecordFormats(), format );

                ExecutionMonitor executionMonitor = verbose ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, out,
                                                                                            SpectrumExecutionMonitor.DEFAULT_WIDTH )
                                                            : ExecutionMonitors.defaultVisible();

                log.info( "### Copy Data ###" );
                log.info( "Source: %s (page cache %s)", from.databaseDirectory(), fromPageCacheMemory );
                log.info( "Target: %s (page cache %s)", toDatabaseLayout.databaseDirectory(), toPageCacheMemory );
                log.info( "Empty database created, will start importing readable data from the source." );

                // Not providing an explicit toPageCache is preferred because then the importer will instantiate
                // its own optimally sized and configured page cache
                try ( PageCache toPageCache = toPageCacheMemory != null ? createPageCache( fs, toPageCacheMemory, scheduler, clock, config ) : null )
                {
                    BatchImporter batchImporter = BatchImporterFactory.withHighestPriority().instantiate(
                            toDatabaseLayout, fs, toPageCache, PageCacheTracer.NULL, Configuration.defaultConfiguration( toDatabaseLayout.databaseDirectory() ),
                            new SimpleLogService( logProvider ), executionMonitor, AdditionalInitialIds.EMPTY, config, recordFormats, NO_MONITOR, scheduler,
                            Collector.EMPTY, TransactionLogInitializer.getLogFilesInitializer(), EmptyMemoryTracker.INSTANCE );

                    batchImporter.doImport( Input.input( () -> nodeIterator( pageCacheTracer ), () -> relationshipIterator( pageCacheTracer ), IdType.INTEGER,
                            getEstimates(), new Groups() ) );
                }

                stats.printSummary();

                // Display schema information
                log.info( "### Extracting schema ###" );
                log.info( "Trying to extract schema..." );

                Collection<String> schemaStatements;
                if ( neoStores.getRecordFormats().hasCapability( FLEXIBLE_SCHEMA_STORE ) )
                {
                    schemaStatements = getSchemaStatements40( stats, neoStores.getSchemaStore(), tokenHolders, cursorTracer );
                }
                else
                {
                    // Prior to 4.0, try to read with 3.5 parser
                    schemaStatements = getSchemaStatements35( log, neoStores.getRecordFormats(), fromPageCache, fs, tokenHolders, cursorTracer );
                }

                int schemaCount = schemaStatements.size();
                if ( schemaCount == 0 )
                {
                    log.info( "... found %d schema definitions.", schemaCount );
                }
                else
                {
                    log.info( "... found %d schema definitions. The following can be used to recreate the schema:", schemaCount );
                    String newLine = System.lineSeparator();
                    log.info( newLine + newLine + String.join( ";" + newLine, schemaStatements ) );
                    log.info( "You have to manually apply the above commands to the database when it is started to recreate the indexes and constraints. " +
                              "The commands are saved to " + logFilePath.toAbsolutePath() + " as well for reference." );

                    if ( schemaCount > 1 )
                    {
                        log.info( "NOTICE: It is recommended to create multiple indexes in the same transaction for more efficient population of those. " +
                                  "This can be done through Embedded or Driver API.");
                    }
                }

                if ( recreatedTokens.notEmpty() )
                {
                    log.info( "The following tokens were recreated (with new names) in order to not leave data behind:" );
                    recreatedTokens.forEach( ( type, tokens ) ->
                                             {
                                                 for ( NamedToken token : tokens )
                                                 {
                                                     log.info( "   `%s` (with id %s(%s)).", token.name(), type, token.id() );
                                                 }
                                             } );
                }
            }
        }
    }

    private TokenHolders createTokenHolders( NeoStores neoStores, PageCursorTracer cursorTracer )
    {
        TokenHolder propertyKeyTokens = createTokenHolder( TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens = createTokenHolder( TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens = createTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        tokenHolders.setInitialTokens( filterDuplicateTokens( allReadableTokens( neoStores ) ), cursorTracer );
        return tokenHolders;
    }

    private TokenHolder createTokenHolder( String tokenType )
    {
        return new RecreatingTokenHolder( tokenType, stats, recreatedTokens );
    }

    private TokensLoader filterDuplicateTokens( TokensLoader loader )
    {
        return new TokensLoader()
        {
            @Override
            public List<NamedToken> getPropertyKeyTokens( PageCursorTracer cursorTracer )
            {
                return unique( loader.getPropertyKeyTokens( cursorTracer ) );
            }

            @Override
            public List<NamedToken> getLabelTokens( PageCursorTracer cursorTracer )
            {
                return unique( loader.getLabelTokens( cursorTracer ) );
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens( PageCursorTracer cursorTracer )
            {
                return unique( loader.getRelationshipTypeTokens( cursorTracer ) );
            }

            private List<NamedToken> unique( List<NamedToken> tokens )
            {
                if ( !tokens.isEmpty() )
                {
                    Set<String> names = new HashSet<>( tokens.size() );
                    int i = 0;
                    while ( i < tokens.size() )
                    {
                        if ( names.add( tokens.get( i ).name() ) )
                        {
                            i++;
                        }
                        else
                        {
                            removeUnordered( tokens, i );
                        }
                    }
                }
                return tokens;
            }

            /**
             * Remove the token at the given index, by replacing it with the last token in the list.
             * This changes the order of elements, but can be done in constant time instead of linear time.
             */
            private void removeUnordered( List<NamedToken> list, int index )
            {
                int lastIndex = list.size() - 1;
                NamedToken endToken = list.remove( lastIndex );
                if ( index < lastIndex )
                {
                    list.set( index, endToken );
                }
            }
        };
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, String memory, JobScheduler jobScheduler, SystemNanoClock clock,
            Config config )
    {
        VersionContextSupplier versionContextSupplier = EmptyVersionContextSupplier.EMPTY;
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory( fileSystem );
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        MemoryAllocator memoryAllocator = MemoryAllocator.createAllocator( ByteUnit.parse( memory ), memoryTracker );
        MuninnPageCache.Configuration configuration = MuninnPageCache.config( memoryAllocator )
                .versionContextSupplier( versionContextSupplier )
                .clock( clock )
                .bufferFactory( new ConfigurableIOBufferFactory( config, memoryTracker ) );
        return new MuninnPageCache( factory, jobScheduler, configuration );
    }

    private Log4jLogProvider getLog( OutputStream out )
    {
        Neo4jLoggerContext context =
                LogConfig.createBuilder( out, verbose ? DEBUG : INFO ).withTimezone( config.get( GraphDatabaseSettings.db_timezone ) ).build();
        return new Log4jLogProvider( context );
    }

    private static Path getLogFilePath( Config config )
    {
        return config.get( logs_directory ).resolve( format( "neo4j-admin-copy-%s.log", new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( new Date() ) ) );
    }

    private static Collection<String> getSchemaStatements40( StoreCopyStats stats, SchemaStore schemaStore, TokenHolders tokenHolders,
            PageCursorTracer cursorTracer )
    {
        Map<String,IndexDescriptor> indexes = new HashMap<>();
        List<ConstraintDescriptor> constraints = new ArrayList<>();
        SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( schemaStore, tokenHolders );
        schemaRuleAccess.indexesGetAllIgnoreMalformed( cursorTracer ).forEachRemaining( i -> indexes.put( i.getName(), i ) );
        schemaRuleAccess.constraintsGetAllIgnoreMalformed( cursorTracer ).forEachRemaining( constraints::add );
        return getSchemaStatements( stats, tokenHolders, indexes, constraints );
    }

    private Collection<String> getSchemaStatements35( Log log, RecordFormats recordFormats, PageCache fromPageCache, FileSystemAbstraction fs,
            TokenHolders tokenHolders, PageCursorTracer cursorTracer )
    {
        Map<String,IndexDescriptor> indexes = new HashMap<>();
        List<ConstraintDescriptor> constraints = new ArrayList<>();

        // Open store with old reader
        LifeSupport life = new LifeSupport();
        try ( SchemaStore35 schemaStore35 = new SchemaStore35( from.schemaStore(), from.idSchemaStore(), config, org.neo4j.internal.id.IdType.SCHEMA,
                new ScanOnOpenReadOnlyIdGeneratorFactory(), fromPageCache, NullLogProvider.getInstance(), recordFormats, immutable.empty() ) )
        {
            schemaStore35.initialise( true, cursorTracer );
            SchemaStorage schemaStorage35 = new SchemaStorage35( schemaStore35 );

            // Load index providers
            Dependencies deps = new Dependencies();
            Monitors monitors = new Monitors();
            deps.satisfyDependencies( fs, config, fromPageCache, NullLogService.getInstance(), monitors, RecoveryCleanupWorkCollector.immediate(),
                                      pageCacheTracer );
            DatabaseExtensionContext extensionContext = new DatabaseExtensionContext( from, DbmsInfo.UNKNOWN, deps );
            Iterable<ExtensionFactory<?>> extensionFactories = GraphDatabaseDependencies.newDependencies().extensions();
            DatabaseExtensions databaseExtensions = life.add( new DatabaseExtensions( extensionContext, extensionFactories, deps, ignore() ) );
            DefaultIndexProviderMap indexProviderMap = life.add( new DefaultIndexProviderMap( databaseExtensions, config ) );
            life.start();

            // Get rules and migrate to latest
            Map<Long,SchemaRule> ruleById = new LinkedHashMap<>();
            schemaStorage35.getAll( cursorTracer ).forEach( rule -> ruleById.put( rule.getId(), rule ) );
            RecordStorageMigrator.schemaGenerateNames( schemaStorage35, tokenHolders, ruleById, cursorTracer );

            for ( Map.Entry<Long,SchemaRule> entry : ruleById.entrySet() )
            {
                SchemaRule schemaRule = entry.getValue();

                if ( schemaRule instanceof IndexDescriptor )
                {
                    IndexDescriptor indexDescriptor = (IndexDescriptor) schemaRule;
                    try
                    {
                        indexDescriptor = (IndexDescriptor) migrateIndexConfig( indexDescriptor, from, fs, fromPageCache, indexProviderMap, log, cursorTracer );
                        indexDescriptor = (IndexDescriptor) upgradeIndexProvider( indexDescriptor );
                        indexes.put( indexDescriptor.getName(), indexDescriptor );
                    }
                    catch ( IOException e )
                    {
                        stats.invalidIndex( indexDescriptor, e );
                    }
                }
                else if ( schemaRule instanceof ConstraintDescriptor )
                {
                    constraints.add( (ConstraintDescriptor) schemaRule );
                }
            }
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to read schema store %s with 3.5 parser", from.schemaStore() ), e );
            return Collections.emptyList();
        }
        finally
        {
            life.shutdown();
        }

        // Convert to cypher statements
        return getSchemaStatements( stats, tokenHolders, indexes, constraints );
    }

    private static Collection<String> getSchemaStatements( StoreCopyStats stats, TokenHolders tokenHolders, Map<String,IndexDescriptor> indexes,
            List<ConstraintDescriptor> constraints )
    {
        TokenRead tokenRead = new ReadOnlyTokenRead( tokenHolders );
        // Here we use a map and insert index first, if it have a backing constraint, it will be replaced when we
        // insert them after since they have the same name
        Map<String,String> schemaStatements = new HashMap<>();
        for ( IndexDescriptor index : indexes.values() )
        {
            try
            {
                if ( !index.isUnique() )
                {
                    schemaStatements.put( index.getName(), SchemaStatementProcedure.createStatement( tokenRead, index ) );
                }
            }
            catch ( Exception e )
            {
                stats.invalidIndex( index, e );
            }
        }
        for ( ConstraintDescriptor constraint : constraints )
        {
            try
            {
                schemaStatements.put( constraint.getName(), SchemaStatementProcedure.createStatement( indexes::get, tokenRead, constraint ) );
            }
            catch ( Exception e )
            {
                stats.invalidConstraint( constraint, e );
            }
        }

        return schemaStatements.values();
    }

    private static RecordFormats setupRecordFormats( RecordFormats fromRecordFormat, FormatEnum format )
    {
        if ( format == FormatEnum.same )
        {
            return findLatestFormatInFamily( fromRecordFormat )
                    .orElseThrow( () -> new IllegalArgumentException( "This version do not support format family " +  fromRecordFormat.getFormatFamily() ) );
        }
        else if ( format == FormatEnum.high_limit )
        {
            try
            {
                return RecordFormatSelector.selectForConfig( Config.defaults( GraphDatabaseSettings.record_format, "high_limit" ),
                                                             NullLogProvider.getInstance() );
            }
            catch ( IllegalArgumentException e )
            {
                throw new IllegalArgumentException( "Unable to load high-limit format, make sure you are using the correct version of neo4j.", e );
            }
        }
        else if ( format == FormatEnum.aligned )
        {
            return PageAligned.LATEST_RECORD_FORMATS;
        }
        else
        {
            return Standard.LATEST_RECORD_FORMATS;
        }
    }

    private static StoreCopyFilter convertFilter( List<String> deleteNodesWithLabels,
                                                  List<String> keepOnlyNodesWithLabels, List<String> skipLabels,
                                                  List<String> skipProperties, List<List<String>> skipNodeProperties, List<List<String>> keepOnlyNodeProperties,
                                                  List<List<String>> skipRelationshipProperties, List<List<String>> keepOnlyRelationshipProperties,
                                                  List<String> skipRelationships,
                                                  TokenHolders tokenHolders, StoreCopyStats stats )
    {
        int[] deleteNodesWithLabelsIds = getTokenIds( deleteNodesWithLabels, tokenHolders.labelTokens() );
        int[] keepOnlyNodesWithLabelsIds = getTokenIds( keepOnlyNodesWithLabels, tokenHolders.labelTokens() );
        int[] skipLabelsIds = getTokenIds( skipLabels, tokenHolders.labelTokens() );
        int[] skipPropertyIds = getTokenIds( skipProperties, tokenHolders.propertyKeyTokens() );
        var skipNodePropertyIds = getPropertyComboTokenIds( skipNodeProperties, tokenHolders.propertyKeyTokens(), tokenHolders.labelTokens(), false );
        var keepOnlyNodePropertyIds = getPropertyComboTokenIds( keepOnlyNodeProperties, tokenHolders.propertyKeyTokens(), tokenHolders.labelTokens(), true );
        var skipRelationshipPropertyIds =
                getPropertyComboTokenIds( skipRelationshipProperties, tokenHolders.propertyKeyTokens(), tokenHolders.relationshipTypeTokens(), false );
        var keepOnlyRelationshipPropertyIds =
                getPropertyComboTokenIds( keepOnlyRelationshipProperties, tokenHolders.propertyKeyTokens(), tokenHolders.relationshipTypeTokens(), true );
        int[] skipRelationshipIds = getTokenIds( skipRelationships, tokenHolders.relationshipTypeTokens() );

        return new StoreCopyFilter( stats, deleteNodesWithLabelsIds, keepOnlyNodesWithLabelsIds, skipLabelsIds, skipPropertyIds, skipNodePropertyIds,
                                    keepOnlyNodePropertyIds, skipRelationshipPropertyIds, keepOnlyRelationshipPropertyIds, skipRelationshipIds );
    }

    private static int[] getTokenIds( List<String> tokenNames, TokenHolder tokenHolder )
    {
        int[] labelIds = new int[tokenNames.size()];
        int i = 0;
        for ( String tokenName : tokenNames )
        {
            int labelId = tokenHolder.getIdByName( tokenName );
            if ( labelId == TokenConstants.NO_TOKEN )
            {
                throw new RuntimeException( "Unable to find token: " + tokenName );
            }
            labelIds[i++] = labelId;
        }
        return labelIds;
    }

    private static ImmutableIntObjectMap<ImmutableIntSet> getPropertyComboTokenIds( List<List<String>> propertiesWithOwningEntityTokens,
            TokenHolder propertyKeyTokens, TokenHolder owningEntityTokens, boolean groupByOwningEntity )
    {
        MutableIntObjectMap<MutableIntSet> propertyComboTokens = new IntObjectHashMap<>();

        for ( List<String> oneOwningEntityTokenAndProperty : propertiesWithOwningEntityTokens )
        {
            if ( oneOwningEntityTokenAndProperty.size() != 2 )
            {
                throw new RuntimeException(
                        "Malformed property combo '" + oneOwningEntityTokenAndProperty + "', should be " + owningEntityTokens.getTokenType() + ".property" );
            }

            int propId = propertyKeyTokens.getIdByName( oneOwningEntityTokenAndProperty.get( 1 ) );
            if ( propId == TokenConstants.NO_TOKEN )
            {
                throw new RuntimeException( "Unable to find token: for " + oneOwningEntityTokenAndProperty.get( 1 ) );
            }
            int owningEntityTokenId = owningEntityTokens.getIdByName( oneOwningEntityTokenAndProperty.get( 0 ) );
            if ( owningEntityTokenId == TokenConstants.NO_TOKEN )
            {
                throw new RuntimeException( "Unable to find token: for " + oneOwningEntityTokenAndProperty.get( 0 ) );
            }

            if ( groupByOwningEntity )
            {
                MutableIntSet propertyTokenIds = propertyComboTokens.getIfAbsentPut( owningEntityTokenId, IntHashSet::new );
                propertyTokenIds.add( propId );
            }
            else
            {
                MutableIntSet owningEntityTokenIds = propertyComboTokens.getIfAbsentPut( propId, IntHashSet::new );
                owningEntityTokenIds.add( owningEntityTokenId );
            }
        }
        MutableIntObjectMap<ImmutableIntSet> immutableSets = new IntObjectHashMap<>();
        propertyComboTokens.forEachKeyValue( ( i, integers ) -> immutableSets.put( i, integers.toImmutable() ) );
        return immutableSets.toImmutable();
    }

    private Input.Estimates getEstimates()
    {
        long propertyStoreSize = storeSize( propertyStore ) / 2 +
                                 storeSize( propertyStore.getStringStore() ) / 2 +
                                 storeSize( propertyStore.getArrayStore() ) / 2;
        return Input.knownEstimates(
                nodeStore.getNumberOfIdsInUse(),
                relationshipStore.getNumberOfIdsInUse(),
                propertyStore.getNumberOfIdsInUse(),
                propertyStore.getNumberOfIdsInUse(),
                propertyStoreSize / 2,
                propertyStoreSize / 2,
                tokenHolders.labelTokens().size() );
    }

    private static long storeSize( CommonAbstractStore<? extends AbstractBaseRecord,? extends StoreHeader> store )
    {
        try
        {
            return store.getStoreSize();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private LenientInputChunkIterator nodeIterator( PageCacheTracer pageCacheTracer )
    {
        return new LenientInputChunkIterator( nodeStore )
        {
            @Override
            public InputChunk newChunk()
            {
                return new LenientNodeReader( stats, nodeStore, propertyStore, tokenHolders, storeCopyFilter, pageCacheTracer );
            }
        };
    }

    private LenientInputChunkIterator relationshipIterator( PageCacheTracer pageCacheTracer )
    {
        return new LenientInputChunkIterator( relationshipStore )
        {
            @Override
            public InputChunk newChunk()
            {
                return new LenientRelationshipReader( stats, relationshipStore, propertyStore, tokenHolders, storeCopyFilter, pageCacheTracer );
            }
        };
    }
}

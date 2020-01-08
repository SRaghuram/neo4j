/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
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
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
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
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.builtin.SchemaStatementProcedure;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.token.api.TokenHolder;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.internal.recordstorage.StoreTokens.readOnlyTokenHolders;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.logging.Level.DEBUG;
import static org.neo4j.logging.Level.INFO;

public class StoreCopy
{
    private final DatabaseLayout from;
    private final Config config;
    private final boolean verbose;
    private final FormatEnum format;
    private final List<String> deleteNodesWithLabels;
    private final List<String> skipLabels;
    private final List<String> skipProperties;
    private final List<String> skipRelationships;
    private final PrintStream out;

    private StoreCopyFilter storeCopyFilter;
    private TokenHolders tokenHolders;
    private NodeStore nodeStore;
    private PropertyStore propertyStore;
    private RelationshipStore relationshipStore;
    private StoreCopyStats stats;

    public enum FormatEnum
    {
        same,
        standard,
        high_limit
    }

    public StoreCopy( DatabaseLayout from, Config config, FormatEnum format, List<String> deleteNodesWithLabels, List<String> skipLabels,
            List<String> skipProperties, List<String> skipRelationships, boolean verbose, PrintStream out )
    {
        this.from = from;
        this.config = config;
        this.format = format;
        this.deleteNodesWithLabels = deleteNodesWithLabels;
        this.skipLabels = skipLabels;
        this.skipProperties = skipProperties;
        this.skipRelationships = skipRelationships;
        this.out = out;
        this.verbose = verbose;
    }

    public void copyTo( DatabaseLayout toDatabaseLayout ) throws Exception
    {
        Path logFilePath = getLogFilePath( config );
        try ( OutputStream logFile = new BufferedOutputStream( Files.newOutputStream( logFilePath ) ) )
        {
            LogProvider logProvider = new DuplicatingLogProvider( getLog( logFile ), getLog( out ) );
            Log log = logProvider.getLog( "StoreCopy" );
            try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                    JobScheduler scheduler = createInitialisedScheduler();
                    PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, scheduler );
                    NeoStores neoStores =
                            new StoreFactory( from, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fs, NullLogProvider.getInstance(),
                                    NULL ).openAllNeoStores(); )
            {
                out.println( "Starting to copy store, output will be saved to: " + logFilePath.toAbsolutePath() );
                nodeStore = neoStores.getNodeStore();
                propertyStore = neoStores.getPropertyStore();
                relationshipStore = neoStores.getRelationshipStore();
                tokenHolders = readOnlyTokenHolders( neoStores );
                stats = new StoreCopyStats( log );
                SchemaStore schemaStore = neoStores.getSchemaStore();

                storeCopyFilter = convertFilter( deleteNodesWithLabels, skipLabels, skipProperties, skipRelationships, tokenHolders, stats );

                RecordFormats recordFormats = setupRecordFormats( neoStores, format );

                ExecutionMonitor executionMonitor = verbose ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, out,
                        SpectrumExecutionMonitor.DEFAULT_WIDTH ) : ExecutionMonitors.defaultVisible();

                log.info( "### Copy Data ###" );
                log.info( "Source: %s", from.databaseDirectory() );
                log.info( "Target: %s", toDatabaseLayout.databaseDirectory() );
                log.info( "Empty database created, will start importing readable data from the source." );

                BatchImporter batchImporter = BatchImporterFactory.withHighestPriority().instantiate( toDatabaseLayout, fs, pageCache, Configuration.DEFAULT,
                        new SimpleLogService( logProvider ), executionMonitor, AdditionalInitialIds.EMPTY, config, recordFormats, NO_MONITOR, null,
                        Collector.EMPTY, TransactionLogsInitializer.INSTANCE );

                batchImporter.doImport( Input.input( this::nodeIterator, this::relationshipIterator, IdType.INTEGER, getEstimates(), new Groups() ) );

                stats.printSummary();
                // Display schema information
                log.info( "### Extracting schema ###" );
                log.info( "Trying to extract schema..." );
                Map<String,String> schemaStatements = getSchemaStatements( stats, schemaStore, tokenHolders );
                log.info( "... found %d schema definition. The following can be used to recreate the schema:", schemaStatements.size() );
                log.info( System.lineSeparator() + System.lineSeparator() + String.join( ";" + System.lineSeparator(), schemaStatements.values() ) );
            }
        }
    }

    private LogProvider getLog( OutputStream out )
    {
        return FormattedLogProvider
                .withZoneId( config.get( GraphDatabaseSettings.db_timezone ).getZoneId() )
                .withDefaultLogLevel( verbose ? DEBUG : INFO )
                .toOutputStream( out );
    }

    private static Path getLogFilePath( Config config )
    {
        return config.get( logs_directory ).resolve( format( "neo4j-admin-copy-%s.log", new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( new Date() ) ) );
    }

    private static Map<String,String> getSchemaStatements( StoreCopyStats stats, SchemaStore schemaStore,
            TokenHolders tokenHolders )
    {
        TokenRead tokenRead = new ReadOnlyTokenRead( tokenHolders );
        SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( schemaStore, tokenHolders );
        Map<String,IndexDescriptor> indexes = new HashMap<>();
        List<ConstraintDescriptor> constraints = new ArrayList<>();
        schemaRuleAccess.indexesGetAll().forEachRemaining( i -> indexes.put( i.getName(), i ) );
        schemaRuleAccess.constraintsGetAllIgnoreMalformed().forEachRemaining(constraints::add );

        Map<String,String> schemaStatements = new HashMap<>();
        for ( var entry : indexes.entrySet() )
        {
            String statement;
            try
            {
                statement = SchemaStatementProcedure.createStatement( tokenRead, entry.getValue() );
            }
            catch ( Exception e )
            {
                stats.invalidIndex( entry.getValue(), e );
                continue;
            }
            schemaStatements.put( entry.getKey(), statement );
        }
        for ( ConstraintDescriptor constraint : constraints )
        {
            String statement;
            try
            {
                statement = SchemaStatementProcedure.createStatement( indexes::get, tokenRead, constraint );
            }
            catch ( Exception e )
            {
                stats.invalidConstraint( constraint, e );
                continue;
            }
            schemaStatements.put( constraint.getName(), statement );
        }
        return schemaStatements;
    }

    private static RecordFormats setupRecordFormats( NeoStores neoStores, FormatEnum format )
    {
        if ( format == FormatEnum.same )
        {
            return neoStores.getRecordFormats();
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
        else
        {
            return Standard.LATEST_RECORD_FORMATS;
        }
    }

    private static StoreCopyFilter convertFilter( List<String> deleteNodesWithLabels, List<String> skipLabels, List<String> skipProperties,
            List<String> skipRelationships, TokenHolders tokenHolders, StoreCopyStats stats )
    {
        int[] deleteNodesWithLabelsIds = getTokenIds( deleteNodesWithLabels, tokenHolders.labelTokens() );
        int[] skipLabelsIds = getTokenIds( skipLabels, tokenHolders.labelTokens() );
        int[] skipPropertyIds = getTokenIds( skipProperties, tokenHolders.propertyKeyTokens() );
        int[] skipRelationshipIds = getTokenIds( skipRelationships, tokenHolders.relationshipTypeTokens() );

        return new StoreCopyFilter( stats, deleteNodesWithLabelsIds, skipLabelsIds, skipPropertyIds, skipRelationshipIds );
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

    private LenientInputChunkIterator nodeIterator()
    {
        return new LenientInputChunkIterator( nodeStore )
        {
            @Override
            public InputChunk newChunk()
            {
                return new LenientNodeReader( stats, nodeStore, propertyStore, tokenHolders, storeCopyFilter );
            }
        };
    }

    private LenientInputChunkIterator relationshipIterator()
    {
        return new LenientInputChunkIterator( relationshipStore )
        {
            @Override
            public InputChunk newChunk()
            {
                return new LenientRelationshipReader( stats, relationshipStore, propertyStore, tokenHolders, storeCopyFilter );
            }
        };
    }
}

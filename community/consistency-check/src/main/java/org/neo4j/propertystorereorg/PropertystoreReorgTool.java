/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.propertystorereorg;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.utils.runutils.CommandProcessor;
import org.neo4j.utils.runutils.IterableStore;
import org.neo4j.utils.runutils.RecordScanner;
import org.neo4j.utils.runutils.StoppableRunnable;
import org.neo4j.utils.runutils.TaskExecutionOrder;
import org.neo4j.store.*;

public class PropertystoreReorgTool
{
    
    private static final String CONFIG = "config";
    private final ProgressMonitorFactory.MultiPartBuilder progress;
    private StoreAccess storeAccess = null;
    StoreFactory storeFactory;
    FileSystemAbstraction fileSys;

    PropertystoreReorgTool( ProgressMonitorFactory.MultiPartBuilder progress, StoreAccess storeAccess,
            StoreFactory storeFactory, FileSystemAbstraction fileSys )
    {
        this.progress = progress;
        this.storeAccess = storeAccess;
        this.storeFactory = storeFactory;
        this.fileSys = fileSys;
    }

    public static void main( String[] args ) 
    { 
        StringLogger logger = StringLogger.SYSTEM;
        try
        {
            Args arguments = Args.withFlags( PropertyReorgSettings.COMMAND ).parse( args );
            String storeDir = determineStoreDirectory( arguments );
            args = getCommands( args );
            System.out.println("------------>["+ "Property/Array/String Store reorg tool"+"] "+ storeDir +"<---------------");
       
            DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            Config tuningConfiguration = readTuningConfiguration( storeDir, arguments );
            ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                    fileSystem, tuningConfiguration, PageCacheTracer.NULL );
            PageCache pageCache = pageCacheFactory.getOrCreatePageCache();
            Monitors monitors = new Monitors();
            StoreFactory factory = new StoreFactory(
                    tuningConfiguration,
                    new DefaultIdGeneratorFactory(),
                    pageCache, fileSystem, logger,
                    monitors
            );

            NeoStore neoStore = factory.newNeoStore( false ) ;
            neoStore.makeStoreOk();
                
            //StoreAccess 
            StoreAccess storeAccessNew = new StoreAccess( neoStore );
            StringBuilder consoleMsg = new StringBuilder(CommandTask.getCommand()[0].name()+" ["+ Version.getKernel().toString()+"]");
            
            ProgressMonitorFactory.MultiPartBuilder progress = ProgressMonitorFactory.textual( System.err ).multipleParts( consoleMsg.toString());
            PropertystoreReorgTool reorgTool = new PropertystoreReorgTool(progress,
                    storeAccessNew, factory, fileSystem);
            reorgTool.run();
        }
        catch ( Exception e )
        {
            //throw new Exception( "property reorg aborted due to exception", e );
            System.out.println("property reorg aborted due to exception:"+e.getMessage() );
        }
        finally
        {
            logger.flush();
        }
    }
   
    private static String[] getCommands(String[] args) throws Exception
    {
        ArrayList<String> argsN = new ArrayList<String>();
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if (args[i].startsWith("-") || args[i].startsWith("--"))
            {
                int offset = args[i].startsWith("--") ? 2 : 1;
                arg = args[i].substring(offset);
            }
            if (arg.equalsIgnoreCase(PropertyReorgSettings.COMMAND))
            {
                i++;
                String[] commands = args[i].split( "," );
                for (String command : commands)
                {
                    if (!CommandTask.setCCCommand( command ))
                        throw new Exception("Unrecognized command: "+ args[i]);
                }
            }
            else
                argsN.add( args[i] );
        }
        return argsN.toArray( new String[0] );
    }
    
    private static String determineStoreDirectory( Args arguments ) throws Exception
    {
        List<String> unprefixedArguments = arguments.orphans();
        String storeDir = unprefixedArguments.get( 0 );
        if ( !new File( storeDir ).isDirectory() )
        {
            throw new Exception( lines( String.format( "'%s' is not a directory", storeDir ) ) + usage() );
        }
        return storeDir;
    }
    private static String lines( String... content )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : content )
        {
            result.append( line ).append( System.getProperty( "line.separator" ) );
        }
        return result.toString();
    }
    private static String usage()
    {
       return "yet to done";
    }
    
    private static Config readTuningConfiguration( String storeDir, Args arguments ) throws Exception
    {
        Map<String,String> specifiedProperties = stringMap();

        String propertyFilePath = arguments.get( CONFIG, null );
        if ( propertyFilePath != null )
        {
            File propertyFile = new File( propertyFilePath );
            try
            {
                specifiedProperties = MapUtil.load( propertyFile );
            }
            catch ( IOException e )
            {
                System.out.println("Error in readTuningConfiguration:"+e.getMessage());
                throw new Exception( String.format( "Could not read configuration properties file [%s]",
                        propertyFilePath ), e );
            }
        }
        specifiedProperties.put( GraphDatabaseSettings.store_dir.name(), storeDir );
        return new Config( specifiedProperties, GraphDatabaseSettings.class, PropertyReorgSettings.class );
    }
    
    public void run()  throws Exception
    {
        List<StoppableRunnable> tasks = null;
        CommandTask.CCCommandType[] commands = CommandTask.getCommand();
        for (CommandTask.CCCommandType command : commands)
        {
            if ( command == CommandTask.CCCommandType.CCNew_GetStats)
                tasks = createTasksForLocalityStats();
            else if ( command == CommandTask.CCCommandType.CCNew_ReorgPropertyStore)
                tasks = createTasksForPropertyFix( false );
            else if ( command == CommandTask.CCCommandType.CCNew_ReorgPropertyStoreLabelBased)
                tasks = createTasksForPropertyFix( true );
    
            TaskExecutionOrder order = new TaskExecutionOrder();
            order.execute( tasks, progress.build() );
        }
    }
    public List<StoppableRunnable> createTasksForPropertyFix( boolean useLabelOrder )
    {
        return createTasksForPropertyFix( null );
    }
    
    public List<StoppableRunnable> createTasksForPropertyFix(List<StoppableRunnable> tasks)
    {
        if (tasks == null)
            tasks = new ArrayList<>();
        //tasks.add( new RecordScanner<PropertyRecord>( new IterableStore<PropertyRecord>( storeAccess
        //        .getPropertyStore(), storeAccess ), "PropertyStore locality - before relocation", progress,
        //        new PropertyStoreLocalityProcess( (PropertyStore) storeAccess.getPropertyStore(), storeAccess ) ) );
        // property relocation
        PropertyStoreRelocatorProcess propertyRelocator = new PropertyStoreRelocatorProcess( storeAccess, storeFactory, fileSys);
        //as part of prepare, save the nodestore and relationshipstore files as they get updated during relocation
        CommandProcessor prepare = new CommandTask( storeAccess, storeFactory, fileSys,
                CommandTask.CCCommandType.CCNew_BackupNodeAndRelationshipStore,
                CommandTask.CCCommandType.CCNew_CreateTempStore);
                
        //Now, relocate the properties - first for nodes and then for relationships
        tasks.add( new RecordScanner<NodeRecord>( new IterableStore<NodeRecord>( storeAccess.getNodeStore(),
                storeAccess ), "Relocate node property chains", progress, propertyRelocator, prepare) );
        
        // as part of cleanup after the property relocation, save the old property store files and move the new property 
        // store files from temp location
        CommandProcessor cleanup = new CommandTask(storeAccess, storeFactory, fileSys,
                CommandTask.CCCommandType.CCNew_SwapTempStore);
        tasks.add( new RecordScanner<RelationshipRecord>( new IterableStore<RelationshipRecord>( storeAccess
                .getRelationshipStore(), storeAccess ), "Relocate relationship property chains", progress, new PropertyStoreRelocatorProcess( storeAccess,
                storeFactory, fileSys), null, cleanup ) );
        return tasks;
    }

    public List<StoppableRunnable> createTasksForPropertyFixNew(List<StoppableRunnable> tasks, boolean useLabelOrder)
    {
        if (tasks == null)
            tasks = new ArrayList<>();
        
        //tasks.add( new RecordScanner<PropertyRecord>( new IterableStore<PropertyRecord>( storeAccess
        //        .getPropertyStore(), storeAccess ), "PropertyStore locality - before relocation", progress,
        //        new PropertyStoreLocalityProcess( (PropertyStore) storeAccess.getPropertyStore(), storeAccess ) ) );
        // property relocation
        PropertyStoreRelocatorProcess propertyRelocator = new PropertyStoreRelocatorProcess( storeAccess, storeFactory, fileSys);
        //as part of prepare, save the nodestore and relationshipstore files as they get updated during relocation
        CommandProcessor prepare = new CommandTask( storeAccess, storeFactory, fileSys,
                CommandTask.CCCommandType.CCNew_BackupNodeAndRelationshipStore,
                CommandTask.CCCommandType.CCNew_CreateTempStore);
                
        //Now, build the node iterable store - if using label order create tasks to buid NodeIdMap
        IterableStore<NodeRecord> nodeIterableStore =  new IterableStore<NodeRecord>( storeAccess.getNodeStore(),
                storeAccess );;
        if (useLabelOrder)
        {
            NodeLabelCountProcess nodeLabelCountProcess = new NodeLabelCountProcess( (NodeStore) storeAccess
                    .getNodeStore(), storeAccess );
            CommandProcessor stateChanger = nodeLabelCountProcess.new StateChanger();
            RecordScanner scanner = new RecordScanner<>( new IterableStore<>( storeAccess.getNodeStore(), storeAccess ),
                    "NodeStore label scan", progress, nodeLabelCountProcess, stateChanger );
            scanner.setParallel( true );
            scanner.setIteration ( 3 );
            tasks.add( scanner );
            nodeIterableStore.setUseNodeIdMap( new NodeIdMapByLabel() );    
        }
        
        //Now, relocate the properties - first for nodes and then for relationships
        tasks.add( new RecordScanner<NodeRecord>( nodeIterableStore, "Relocate node property chains", progress, propertyRelocator, prepare) );
        
        // as part of cleanup after the property relocation, save the old property store files and move the new property 
        // store files from temp location
        CommandProcessor cleanup = new CommandTask(storeAccess, storeFactory, fileSys,
                CommandTask.CCCommandType.CCNew_SwapTempStore);
        tasks.add( new RecordScanner<RelationshipRecord>( new IterableStore<RelationshipRecord>( storeAccess
                .getRelationshipStore(), storeAccess ), "Relocate relationship property chains", progress, new PropertyStoreRelocatorProcess( storeAccess,
                storeFactory, fileSys), null, cleanup ) );
        return tasks;
    }

    public List<StoppableRunnable> createTasksForLocalityStats()
    {
        return createTasksForLocalityStats( null );
    }
    
    public List<StoppableRunnable> createTasksForLocalityStats(List<StoppableRunnable> tasks)
    {
        if (tasks == null)
            tasks = new ArrayList<>();
        tasks.add( new RecordScanner<>( new IterableStore<>( storeAccess.getPropertyStore(), storeAccess ),
                "PropertyStore locality", progress, new PropertyStoreLocalityProcess( (PropertyStore) storeAccess
                        .getPropertyStore(), storeAccess ) ) );
        return tasks;
    }
    
    public List<StoppableRunnable> createTasksForPropertyFixLabelBased()
    {
        return createTasksForPropertyFixLabelBased( null );
    }
    
    
    public List<StoppableRunnable> createTasksForPropertyFixLabelBased(List<StoppableRunnable> tasks)
    {
        if (tasks == null)
            tasks = new ArrayList<>();
        NodeLabelCountProcess nodeLabelCountProcess = new NodeLabelCountProcess( (NodeStore) storeAccess
                .getNodeStore(), storeAccess );
        CommandProcessor stateChanger = nodeLabelCountProcess.new StateChanger();
        RecordScanner scanner = new RecordScanner<>( new IterableStore<>( storeAccess.getNodeStore(), storeAccess ),
                "NodeStore label scan", progress, nodeLabelCountProcess, stateChanger );
        scanner.setParallel( true );
        tasks.add( scanner );
        tasks.add( scanner );
        tasks.add( scanner );
        
        IterableStore iterableStore = new IterableStore<NodeRecord>( storeAccess.getNodeStore(),
                storeAccess );
        iterableStore.setUseNodeIdMap( new NodeIdMapByLabel() );
        
        PropertyStoreRelocatorProcess propertyRelocator = new PropertyStoreRelocatorProcess( storeAccess, storeFactory, fileSys);
        //as part of prepare, save the nodestore and relationshipstore files as they get updated during relocation
        CommandProcessor prepare = new CommandTask( storeAccess, storeFactory, fileSys,
                CommandTask.CCCommandType.CCNew_BackupNodeAndRelationshipStore,
                CommandTask.CCCommandType.CCNew_CreateTempStore);
                
        //Now, relocate the properties - first for nodes and then for relationships
        tasks.add( new RecordScanner<NodeRecord>( iterableStore, "Relocate node property chains", progress, propertyRelocator, prepare) );
        
        // as part of cleanup after the property relocation, save the old property store files and move the new property 
        // store files from temp location
        CommandProcessor cleanup = new CommandTask(storeAccess, storeFactory, fileSys,
                CommandTask.CCCommandType.CCNew_SwapTempStore);
        tasks.add( new RecordScanner<RelationshipRecord>( new IterableStore<RelationshipRecord>( storeAccess
                .getRelationshipStore(), storeAccess ), "Relocate relationship property chains", progress, new PropertyStoreRelocatorProcess( storeAccess,
                storeFactory, fileSys ), null, cleanup ) );
       
        return tasks;
    }
}

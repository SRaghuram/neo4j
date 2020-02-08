package org.neo4j.internal.batchimport;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.input.*;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.MyStoreVersion;

import java.io.IOException;
import java.util.ArrayList;

import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;

@ServiceProvider
public class MyBatchImportFactory implements BatchImporterFactory {

    public enum STATE
    {
        UNINITIALIZED,
        NODE,
        RELATIONSHIP,
        REVISIT_NODE,
        REVISIT_RELATIONSHIP
    }

    class MyAdapter implements InputEntityVisitor
    {
        MyStore myStore = null;
        public STATE state = STATE.UNINITIALIZED;
        ArrayList<String> propNames = new ArrayList<String>(), propValues = new ArrayList<String>();
        ArrayList<MyStore.DATATYPE> propTypes = new ArrayList<MyStore.DATATYPE>();
        String idPropName = null, idPropValue = null, relType = null, idGroupName = null, source = null, dst = null;
        String[] labels = null;
        boolean processId = false;
        public void clearData()
        {
            labels = null;
            propNames.clear();
            propValues.clear();
            idPropName = null;
            idPropValue = null;
            relType = null;
            idGroupName = null;
            source = null;
            dst = null;
        }

        public MyAdapter setStore(MyStore store)
        {
            this.myStore = store;
            return this;
        }
        public MyAdapter setState(STATE state)
        {
            this.state = state;
            return this;
        }
        @Override
        public boolean property( String key, Object value)
        {
            return property( key, value, null);
        }
        @Override
        public boolean property( String key, Object value, Object strValue )
        {
            String valueStr = null;//MyStore.StringFromValue(value, strValue);
            if (processId )
            {
                idPropName = key;
                idPropValue = (String)strValue;
                processId = false;
            }
            else
            {
                propTypes.add( MyStore.valueType(value));
                propNames.add(key);
                if (strValue instanceof String[])
                {
                   String[] vals = (String[])strValue;
                   for (String val: vals)
                       valueStr += val+";";
                    propValues.add(valueStr);

                } else
                    propValues.add((String)strValue);
            }
           // System.out.println("property:"+"state["+ state+"]"+key+"-"+ ((valueStr == null) ? strValue : valueStr));
            return true;
        }

        @Override
        public boolean property( int propertyKeyId, Object value )
        {
            System.out.println("Property:"+"state["+ state+"]"+propertyKeyId+"-"+ value);
            return true;
        }

        @Override
        public boolean propertyId( long nextProp )
        {
            System.out.println("propetyId:"+ "state["+ state+"]"+nextProp);return true;
        }

        @Override
        public boolean id( long id )
        {
            System.out.println("id:"+"state["+ state+"]"+id);return true;
        }

        @Override
        public boolean id( Object id, Group group )
        {
            //System.out.println("id:"+"state["+ state+"]"+ id  + "-"+ group);
            idGroupName = group.name();
            processId = true;
            return true;
        }

        @Override
        public boolean labels( String[] labels )
        {
            this.labels = labels;
            StringBuilder str = new StringBuilder();
            for (String lbl : labels)
                str.append(lbl+":");
            //System.out.println("labels:"+"state["+ state+"]"+str.toString());

            return true;
        }

        @Override
        public boolean startId( long id )
        {
            System.out.println("startId:"+"state["+ state+"]"+id);
            return true;
        }

        @Override
        public boolean startId( Object id, Group group )
        {
            //System.out.println("startId:"+ "state["+ state+"]"+id  + "-"+ group);
            source = (String)id;
            return true;
        }

        @Override
        public boolean endId( long id )
        {
            System.out.println("endId:"+"state["+ state+"]"+ id);
            return true;
        }

        @Override
        public boolean endId( Object id, Group group )
        {
            //System.out.println("endId:"+"state["+ state+"]"+ id  + "-"+ group);
            dst = (String)id;
            return true;
        }

        @Override
        public boolean type( int type )
        {
            System.out.println("type:"+"state["+ state+"]"+type);
            return true;
        }

        @Override
        public boolean type( String type )
        {
            //System.out.println("type:"+"state["+ state+"]"+type);
            relType = type;
            return true;
        }

        @Override
        public boolean labelField( long labelField )
        {
            System.out.println("lableField:"+"state["+ state+"]"+ labelField);
            return true;
        }

        @Override
        public void endOfEntity()
        {
            String[] pNames =  propNames.toArray(new String[0]);
            String[] pValues = propValues.toArray(new String[0]);
            MyStore.DATATYPE[] pTypes = propTypes.toArray(new MyStore.DATATYPE[0]);
            if (this.state == STATE.NODE)
                myStore.addNode(idPropName, idPropValue, labels, pNames, pValues, pTypes);
            if (this.state == STATE.RELATIONSHIP)
                myStore.addRel(relType, source,dst, pNames, pValues, pTypes );

            if (this.state == STATE.REVISIT_NODE)
            {
                StringBuilder str = new StringBuilder();
                str.append("["+idPropName+":"+idPropValue+"][labels:");
                for (int i= 0; i < labels.length;i++)
                    str.append(labels[i]+",");
                str.append("]");
                for (int i= 0; i < pNames.length;i++)
                    str.append("["+pNames[i]+":"+pValues[i]+"]");
                System.out.println("Node:"+str.toString());
            }
            if (this.state == STATE.REVISIT_RELATIONSHIP)
            {
                int relId = myStore.getKeyId( relType, MyStore.KEYTYPE.RELTYPE);
                StringBuilder str = new StringBuilder();
                str.append("["+relType+"]["+source+"-->"+dst+"]");
                for (int i= 0; i < pNames.length;i++)
                    str.append("["+pNames[i]+":"+pValues[i]+"]");
                System.out.println("Rel:"+str.toString());
            }
            this.setState(STATE.UNINITIALIZED);
            this.clearData();
            //System.out.println("endOfEntity"+" state["+ state+"]");
        }

        @Override
        public void close()
        {
            System.out.println("close "+"state["+ state+"]");
        }
    }


    @Override
    public BatchImporter instantiate() {
        return null;
    }

    DatabaseLayout directoryStructure;
    FileSystemAbstraction fileSystem;
    PageCache pageCache;
    Configuration config;
    LogService logService;
    ExecutionMonitor executionMonitor;
    AdditionalInitialIds additionalInitialIds;
    Config dbConfig;
    ImportLogicMonitor.Monitor monitor;
    JobScheduler jobScheduler;
    Collector badCollector;
    LogFilesInitializer logFilesInitializer;

    @Override
    public BatchImporter instantiate(DatabaseLayout directoryStructure, FileSystemAbstraction fileSystem, PageCache externalPageCache, Configuration config,
                                     LogService logService, ExecutionMonitor executionMonitor, AdditionalInitialIds additionalInitialIds, Config dbConfig,
                                     ImportLogicMonitor.Monitor monitor, JobScheduler jobScheduler, Collector badCollector, LogFilesInitializer logFilesInitializer) {

        this.directoryStructure = directoryStructure;
        this.fileSystem = fileSystem;
        if (externalPageCache == null)
        {
            SingleFilePageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory( fileSystem );
            pageCache = new MuninnPageCache( swapperFactory,
                    Integer.parseInt( dbConfig.get( pagecache_memory ) == null? "1000000000": dbConfig.get( pagecache_memory )), new DefaultPageCacheTracer(),
                    EmptyVersionContextSupplier.EMPTY, jobScheduler );
        }
        else
            pageCache = externalPageCache;
        this.config = config;
        this.logService = logService;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.dbConfig = dbConfig;
        this.monitor = monitor;
        this.jobScheduler = jobScheduler;
        this.badCollector = badCollector;
        this.logFilesInitializer = logFilesInitializer;

        BatchImporter batchImporter =  new BatchImporter() {
            @Override
            public void doImport(Input input) throws IOException {
                MyStore myStore = new MyStore(fileSystem, directoryStructure, dbConfig, pageCache, null, MyStoreVersion.MyStandardFormat, true);
                MyAdapter visitor = new MyAdapter().setStore( myStore );
                System.out.println("Entered MyBatchImportFactory - doImport");
                InputIterator data = input.nodes(badCollector).iterator();
                try ( InputChunk chunk = data.newChunk() )
                {
                    while ( data.next( chunk ) )
                    {
                        int count = 0;
                        while ( chunk.next( visitor.setState(STATE.NODE) ) )
                        {
                            count++;
                        }
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                catch ( Throwable e )
                {
                    throw e;
                }
                finally
                {
                    visitor.close();
                }

                data = input.relationships(badCollector).iterator();
                try ( InputChunk chunk = data.newChunk() )
                {
                    while ( data.next( chunk ) )
                    {
                        int count = 0;
                        while ( chunk.next( visitor.setState(STATE.RELATIONSHIP) ) )
                        {
                            count++;
                        }
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                catch ( Throwable e )
                {
                    throw e;
                }
                finally
                {
                    visitor.close();
                }

                /*==================
                data = input.relationships(badCollector).iterator();
                try ( InputChunk chunk = data.newChunk() )
                {
                    while ( data.next( chunk ) )
                    {
                        int count = 0;
                        while ( chunk.next( visitor.setState(STATE.REVISIT_RELATIONSHIP) ) )
                        {
                            count++;
                        }
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                catch ( Throwable e )
                {
                    throw e;
                }
                finally
                {
                    visitor.close();
                }

                data = input.nodes(badCollector).iterator();
                try ( InputChunk chunk = data.newChunk() )
                {
                    while ( data.next( chunk ) )
                    {
                        int count = 0;
                        while ( chunk.next( visitor.setState(STATE.REVISIT_NODE) ) )
                        {
                            count++;
                        }
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                catch ( Throwable e )
                {
                    throw e;
                }
                finally
                {
                    visitor.close();
                }
                //==================

                 */

                System.out.println("Import done - Nodes ["+myStore.getNumNodes()+"] Relationships ["+myStore.getNumRels()+"] Properties ["+ myStore.getNumProps()+"]");
                System.out.println("Building links...");
                myStore.buildLinks();
                System.out.println("Saving store...");
                myStore.saveStore();
                //MyStore myStore1 = new MyStore("/Users/sraghuram/myws/4x/4.0-Oct14/neo4j/public/community/server/src/main/java/org/neo4j/server/aspect", false);
                //System.out.println("Reading done - Nodes ["+myStore1.getNumNodes()+" Relationships ["+myStore1.getNumRels()+"] Properties ["+ myStore1.getNumProps()+"]");
            }
        };

        return batchImporter;
    }
}

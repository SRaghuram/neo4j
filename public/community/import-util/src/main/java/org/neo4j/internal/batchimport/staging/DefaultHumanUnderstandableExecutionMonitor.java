package org.neo4j.internal.batchimport.staging;

public class DefaultHumanUnderstandableExecutionMonitor extends BaseHumanUnderstandableExecutionMonitor {
    public DefaultHumanUnderstandableExecutionMonitor(Monitor monitor) {
        super(monitor);
    }

    @Override
    public void start(StageExecution execution) {
        super.tryStart( execution );
    }
}

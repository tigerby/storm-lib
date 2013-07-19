package storm;

import backtype.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This demo scheduler make sure a spout named <code>0_Tx_Spout</code> in topology <code>ollehTV_Live_Ratings_Pre-processing_Topology_OTV_STANDBY</code> runs
 * on a supervisor named <code>daisy08</code>. supervisor does not have name? You can configure it through
 * the config: <code>supervisor.scheduler.meta</code> -- actually you can put any config you like in this config item.
 * 
 * In our example, we need to put the following config in supervisor's <code>storm.yaml</code>:
 * <pre>
 *     # give our supervisor a name: "daisy08"
 *     supervisor.scheduler.meta:
 *       name: "daisy08"
 * </pre>
 * 
 * Put the following config in <code>nimbus</code>'s <code>storm.yaml</code>:
 * <pre>
 *     # tell nimbus to use this custom scheduler
 *     storm.scheduler: "storm.DesignationScheduler"
 * </pre>
 * @author xumingmingv May 19, 2012 11:10:43 AM
 *
 *
 *
 * # scheduler
 * storm.scheduler: storm.DesignationScheduler
 * designation.scheduler.meta:
 *     "ollehTV_Live_Ratings_Pre-processing_Topology_OTV_STANDBY":
 *         "0_Tx_Spout_":
 *             - "daisy08:6700"
 *         "3_Tx_Committer_Bolt_":
 *             - "daisy08:6701"
 *
 *
 */
public class DesignationScheduler implements IScheduler {
    /**
     * SF4J Logger.
     */
    private static Logger logger = LoggerFactory.getLogger(DesignationScheduler.class);

    /**
     * Storm configuration.
     */
    private Map conf;

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        Map<String, Map<String, List<String>>> scheduleIntelli = (Map<String, Map<String, List<String>>>) conf.get("designation.scheduler.meta");
        for(Map.Entry<String, Map<String,List<String>>> topologyIntelli: scheduleIntelli.entrySet()) {
            String topologyName = topologyIntelli.getKey();
            Map<String, List<String>> topologyIntelliValue = topologyIntelli.getValue();

            TopologyDetails topology = topologies.getByName(topologyName);
            for(Map.Entry<String, List<String>> componentIntelli: topologyIntelliValue.entrySet()) {
                String componentName = componentIntelli.getKey();
                List<String> slots = componentIntelli.getValue();

                if (topology != null && cluster.needsScheduling(topology)) {
                    logger.info("DesignationScheduler: begin scheduling");

                    // find out all the needs-scheduling components of this topology
                    Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                    logger.info("needs scheduling(component->executor): " + componentToExecutors);
                    logger.info("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
//                    SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName(topologyName).getId());
//                    if (currentAssignment != null) {
//                        logger.info("current assignments: " + currentAssignment.getExecutorToSlot());
//                    } else {
//                        logger.info("current assignments: {}");
//                    }

                    if (componentToExecutors.containsKey(componentName)) {
                        logger.info("start scheduling {}", componentName);
                        List<ExecutorDetails> executors = componentToExecutors.get(componentName);

                        for(String slot: slots) {
                            String[] expectedSlot = slot.split(":");
                            String expectedHost = expectedSlot[0];
                            String expectedPort = null;
                            if(expectedSlot.length > 1) {
                                expectedPort = expectedSlot[1];
                            }

                            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                            SupervisorDetails foundSupervisor = null;
                            for (SupervisorDetails supervisor : supervisors) {
                                if (expectedHost.equals(supervisor.getHost())) {
                                    foundSupervisor = supervisor;
                                    break;
                                }
                            }
//
                            if (foundSupervisor != null) {
                                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(foundSupervisor);

                                // if there is no available slots on this supervisor, free some.
                                // TODO for simplicity, we free all the used slots on the supervisor.
                                if (availableSlots.isEmpty() && !executors.isEmpty()) {
                                    for (Integer port : cluster.getUsedPorts(foundSupervisor)) {
                                        cluster.freeSlot(new WorkerSlot(foundSupervisor.getId(), port));
                                    }
                                }

                                // re-get the availableSlots
                                availableSlots = cluster.getAvailableSlots(foundSupervisor);

                                if(availableSlots != null && availableSlots.size() > 0) {
                                    WorkerSlot foundSlot = null;
                                    if(expectedPort != null) {
                                        for(WorkerSlot workerSlot: availableSlots) {
                                            if(Integer.parseInt(expectedPort) == workerSlot.getPort()) {
                                                foundSlot = workerSlot;
                                                break;
                                            }
                                        }
                                    } else {
                                        foundSlot = availableSlots.get(0);
                                    }

                                    cluster.assign(foundSlot, topology.getId(), executors);
                                    logger.info("assigned executors: {}({}) to slot: [{}({}):{}]", new Object[] {componentName, executors, foundSupervisor.getHost(), foundSlot.getNodeId(), foundSlot.getPort()});
                                }
                            } else {
                                logger.info("There is no supervisor named {}", expectedHost);
                            }
                        }
                    }
                } else {
                    logger.info("there is no topology to need scheduling.");
                }
            }
        }



        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

}

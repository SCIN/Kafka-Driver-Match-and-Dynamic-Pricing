package com.cloudcomputing.samza.pitt_cabs;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String , Map<String, Object> > driverLocStore;
    private KeyValueStore<String , Map<Integer, Object> > priceStore;
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverLocStore = (KeyValueStore<String, Map<String, Object>>) context.getStore("driver-loc");
        priceStore = (KeyValueStore<String, Map<Integer, Object>>) context.getStore("price");
        for(int i = 1; i < 6; i++) {
            Map<Integer, Object> initPrice = new HashMap<>();
            initPrice.put(1, 1)
        }
        priceStore.put("1", );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a
        // particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a
        // blockId will arrive
        // at one task only, thereby enabling you to do stateful stream
        // processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Map<String, Object> message = (Map<String, Object>)envelope.getMessage();
        int blockId = (int)message.get("blockId");
        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // Handle Driver Location messages
            int driverId = (int)message.get("driverId");
            if(driverLocStore.get(blockId+":"+driverId) != null) {
                driverLocStore.get(blockId+":"+driverId).put("latitude", message.get("latitude"));
                driverLocStore.get(blockId+":"+driverId).put("longitude", message.get("longitude"));
            }
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            String type = (String) message.get("type");
            if(type.equals("RIDE_REQUEST")) {
                processRequestEvent(message, collector, blockId);
            }
            else {
                int driverId = (int)message.get("driverId");
                if(type.equals("ENTERING_BLOCK")) {
                    if(message.get("status").equals("AVAILABLE")) {
                        driverLocStore.put(blockId+":"+driverId, message);
                    }
                }
                else if(type.equals("LEAVING_BLOCK")) {
                    driverLocStore.delete(blockId+":"+driverId);
                }
                else if(type.equals("RIDE_COMPLETE")) {
                    driverLocStore.put(blockId+":"+driverId, message);
                }
                else {
                    System.out.println("Type error!");
                    return;
                }
            }
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void processRequestEvent(Map<String, Object> message, MessageCollector collector, int blockId) {
        int clientId = (int) message.get("clientId");
        String gender_preference = (String) message.get("gender_preference");
        double latitude = (double) message.get("latitude");
        double longitude = (double) message.get("longitude");
        int matchDriverId = 0;
        double matchScore = 0.0;
        KeyValueIterator<String, Map<String, Object>> drivers = driverLocStore.range(blockId+":", blockId+";");
        Map<String, Object> driver = null;
        while (drivers.hasNext()) {
            try {
                driver = drivers.next().getValue();
                if(message.get("status").equals("UNAVAILABLE")) {
                    continue;
                }
                double client_driver_distance = Math.sqrt(Math.pow(latitude - (double) driver.get("latitude"), 2) + Math.pow(longitude - (double) driver.get("longitude"), 2));
                double distance_score = Math.exp(-1 * client_driver_distance);
                double gender_score = 0.0;
                if (gender_preference.equals("N") || gender_preference.equals((String) driver.get("gender"))) {
                    gender_score = 1.0;
                }
                double salary_score = 1 - (int)driver.get("salary") / 100.0;
                double rating_score = (double) driver.get("rating") / 5.0;
                double score = distance_score * 0.4 + gender_score * 0.2 + salary_score * 0.2 + rating_score * 0.2;
                if(clientId == 4100 || clientId == 4112) {
                    System.out.println(clientId +":"+ driver.toString());
                    System.out.println(latitude+","+longitude+","+gender_preference);
                    System.out.printf("%f %f %f %f\n", distance_score, gender_score, salary_score, rating_score);
                }
                if (score >= matchScore) {
                    matchDriverId = (int) driver.get("driverId");
                    matchScore = score;
                }

            }catch (Exception e) {
                e.printStackTrace();
//                System.out.println(driver.toString());
            }
        }
        driverLocStore.delete(blockId+":"+matchDriverId);
        Map<String, Integer> result = new HashMap<>();
        result.put("clientId", clientId);
        result.put("driverId", matchDriverId);
        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, result));
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }
}

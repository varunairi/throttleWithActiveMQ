Multiple scenarios tested here with Active MQ
Queue Based Throttling: 
   Strategy: To allow "producers" that are fast to push down messages to a broker (ActiveMQ)
             The Consumers in the backend to avoid getting overwhelmed, will listen on a queue. After recieving a message, the consumer can give it to a FixedThreadPool and once all are completed (the "Futures" are complete), start polling again
             Multiple Consumers can listen to same queue and only one consumer will get a message at a time . 
             Inside of a single JVM (Consumer JVM), you can setup multiple Queue Listeners and that would be a throttle.  OR
             the Fixed Thread Pool using ExecutorSErvice can work as a Throttle as well OR
             a blocking queue
  

package com.varun.activemq.queue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sinple jms producer and consumer with activemq running in its own process @ localhost:61616.
 * There is a simpler way if everything is in same JVM shown below using "vm" transport where no sockets are used
 */
public class ClientProducer {

    public static void main(String[] args) throws  Exception {

        String brokerUrl = "tcp://localhost:61616?wireFormat.maxFrameSize=104857600&wireFormat.cacheEnabled=true" +
                "&jms.prefetchPolicy.queuePrefetch=10";
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //NO Socket Method
        // ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?create=true");
        Connection queueConn  = connectionFactory.createConnection();
        queueConn.start();

        Session session = queueConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue q = session.createQueue("MyQueue");
        //producer starts
        Thread producerThread = new Thread(new Producer(session, q));
        producerThread.start();


        Thread consumerThread1 = new Thread(new Consumer(session, q));
        Thread consumerThread2 = new Thread(new Consumer(session, q));

        //2 threads for consumers started
        //each thread waits a second to simulate a slow consumer
        consumerThread1.start();
        consumerThread2.start();

        System.out.println("All Threads Started");
        producerThread.join();
        /*consumerThread1.join();
        consumerThread2.join();
*/
        Thread.sleep(10000);
        session.close();
        queueConn.close();


    }

    private static class Producer implements  Runnable{
        private Session producerSession;
        private Queue q;
        public Producer(Session s, Queue q)
        {
            producerSession=s;
            this.q=q;
        }

        public void run() {
            try {
                MessageProducer producer = this.producerSession.createProducer(q);
              //  producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                TextMessage m = this.producerSession.createTextMessage();
                for (int i = 0; i < 10; i ++) {
                    m.setText("This is " + i + "th message by " + Thread.currentThread().getName());
                    System.out.println("This is " + i + "th message by " + Thread.currentThread().getName());
                    producer.send(m);

                }
                producer.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }



private static  class Consumer implements Runnable, ExceptionListener{

        private Session session;
        private Destination destination;

        Consumer(Session session, Destination destination){
            this.session = session;
            this.destination = destination;
        }
    public void run() {
        javax.jms.MessageConsumer consumer = null;
        try {
            consumer = this.session.createConsumer(this.destination);


            while (true){
                TextMessage m = (TextMessage)consumer.receive(1000);
                if (m!=null)
                  System.out.println("Recieved by " +  Thread.currentThread().getName() + " :" + m.getText());
                Thread.sleep(1000);

            }
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                consumer.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    public void onException(JMSException exception) {
        System.out.println(exception.getLocalizedMessage());
    }
}
}



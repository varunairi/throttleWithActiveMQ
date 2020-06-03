package com.varun.activemq.topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.ConsumerThread;

import javax.jms.*;

public class PubSub {

    public static void main(String[] args) throws JMSException, InterruptedException {
        ActiveMQConnectionFactory mqConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        final Connection topicConnection = mqConnectionFactory.createConnection();
        final Session session = topicConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("mytopic");
        topicConnection.start();


        Thread producer = new Thread (new TopicProducer(session, topic));
        producer.start();

        Thread consumerThread1 = new Thread(new TopicConsumer(session, topic, true));
        Thread consumerThread2 = new Thread(new TopicConsumer(session, topic,false));
        Thread consumerThread3 = new Thread(new TopicConsumer(session, topic,false));
        consumerThread1.start();
        consumerThread2.start();
        consumerThread3.start();

        Thread.sleep(20000);
        producer.join();;
        session.close();
        topicConnection.close();
    }

    private static class TopicProducer implements  Runnable{
        private Session session;
        private Destination topicDestination;

        public TopicProducer(Session session, Destination topicDestination) {
            this.session = session;
            this.topicDestination = topicDestination;
        }

        public void run() {
            MessageProducer producer=null;
            try {
                producer = session.createProducer(topicDestination);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                TextMessage textMessage = session.createTextMessage();

                for (int i = 0; i < 10; i++) {
                    String s = " Broadcasting Important Message: Sending Roman Soldier number : " + i;
                    textMessage.setText(s);
                    producer.send(textMessage);
                    System.out.println("Sending " + s);

                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

     static final class TopicConsumer implements  Runnable{
        private Session session;
        private Destination destination;
        private boolean strangeBehavior;


         public TopicConsumer(Session session, Destination destination, boolean strangeBehavior) {
             this.session = session;
             this.destination = destination;
             this.strangeBehavior = strangeBehavior;
         }

         public void run() {
             MessageConsumer consumer = null;
             try {
                 consumer = session.createConsumer(destination,"");
                 while (true) {
                     TextMessage receive = (TextMessage) consumer.receive();
                     System.out.println("Recieved by " + Thread.currentThread().getName() + " : " + receive.getText());
                     Thread.sleep(1000);
                     if(strangeBehavior)
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
     }
}

package com.varun.activemq.queue.amqp;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.soap.Text;
import java.util.Hashtable;

public class AMQPWithProton {

    public static void main(String[] args) throws NamingException, JMSException {
        tryAMQPWithJNDI();
        tryAMQPWithJNDIWithoutProps();
    }

    /**
     * See JNDI.properties
     * @throws NamingException
     * @throws JMSException
     */
    public static void tryAMQPWithJNDI() throws NamingException, JMSException{
        Context ctx = new InitialContext();
        ConnectionFactory factory = (ConnectionFactory) ctx.lookup("myFactoryLookup");

        Connection conn = factory.createConnection(); //there is Overloaded method too to pass in User Name, Password

        Destination queue = (Destination) ctx.lookup("queue1");
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer messageConsumer = session.createConsumer(queue);

        MessageProducer producer = session.createProducer(queue);

        producer.send(session.createTextMessage("Hello From Another world"));


        Message msg = messageConsumer.receive(100000);
        if (msg != null)
            System.out.println("message is : " + ((TextMessage)msg).getText());
        producer.close();
        messageConsumer.close();
        conn.close();
    }


    /**
     * WITHOUT JNDI.properties
     * @throws NamingException
     * @throws JMSException
     */
    public static void tryAMQPWithJNDIWithoutProps() throws NamingException, JMSException{
        Hashtable<Object, Object> table = new Hashtable<Object, Object>();
        table.put(Context.INITIAL_CONTEXT_FACTORY,"org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        table.put("queue.queue1", "MyQueue");
        table.put("connectionfactory.myFactoryLookup","amqp://localhost:5672?jms.clientID=propsNotThere&failover.maxReconnectAttempts=20");
        Context ctx = new InitialContext(table);
        ConnectionFactory factory = (ConnectionFactory) ctx.lookup("myFactoryLookup");

        Connection conn = factory.createConnection(); //there is Overloaded method too to pass in User Name, Password

        Destination queue = (Destination) ctx.lookup("queue1");
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer messageConsumer = session.createConsumer(queue);

        MessageProducer producer = session.createProducer(queue);

        producer.send(session.createTextMessage("Hello From Another world without properties"));


        Message msg = messageConsumer.receive(100000);
        if (msg != null)
            System.out.println("message is : " + ((TextMessage)msg).getText());
        producer.close();
        messageConsumer.close();
        conn.close();
    }

}

package org.apache.hadoop.io.erasurecode.rawcoder;

import java.util.Random;
import java.util.Hashtable;

// JMS 
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.BytesMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

// Most of code imported from VFProxy.MockVFProxyClient

public class OpaeCoderConnector {

    // TODO: export hard-coded parameters here, ideally in a dynamic config file
    private static String jndiFactoryInital = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private static String providerURL = "tcp://127.0.0.1:61616";
    private static String [] PCIesbdfArray = { "0000:01:00.1" };
    // private static String [] PCIesbdfArray = {
    //                                             "0000:01:00.1",
    //                                             "0000:01:00.2",
    //                                             "0000:01:00.3",
    //                                             "0000:01:00.4",
    //                                             "0000:01:00.5"
    //                                         };

    // JMS dynamic context
    private Context         context;
    private QueueSender     sender;
    private QueueReceiver   receiver;
    private QueueSession    session;
    private MapMessage      requestMapMessage;
    private Queue           queueRequest ;
    private Queue           queueResponse;

    // Spawn a new Connection and Session
    // NOTE: some of this might be redundant, 
    // e.g. Conncetion might be shared across Queues and Sessions
    public void connectToJMSProvider () throws NamingException, JMSException {
        // Select a VF randomly
        Random random = new Random();
        String sbdfString = PCIesbdfArray [ random.nextInt(PCIesbdfArray.length) ];
        // Connect to provider
        Hashtable <String, String> p = new Hashtable <String, String>();
        p.put("java.naming.factory.initial", jndiFactoryInital );
        p.put("java.naming.provider.url", providerURL );

        // Queues names
        p.put("queue.Request"  + sbdfString, "Request"  + sbdfString );
        p.put("queue.Response" + sbdfString, "Response" + sbdfString );
        
        // Create context
        Context context = new InitialContext ( p );
        // Lookup queues
        queueRequest  = (Queue) context.lookup("Request"  + sbdfString );
        queueResponse = (Queue) context.lookup("Response" + sbdfString );
        // Lookup (Factory) and start Connection
        QueueConnectionFactory connectionFactory = (QueueConnectionFactory)context.lookup("QueueConnectionFactory");
        QueueConnection connection = connectionFactory.createQueueConnection();
        connection.start();
        
        // Create single-threaded session
        // NOTE: this can't be shared across threads
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        requestMapMessage = session.createMapMessage();
        
        // Setup Sender for queueRequest
        sender = session.createSender(queueRequest);
        // Setup Receiver for queueResponse
        receiver = session.createReceiver(queueResponse);
    }

    // sendMessage, ByteBufferEncodingState version
    public void sendMessage ( 
                       ByteBufferEncodingState encodingState,
                       byte[] survival_pattern,
                       byte[] erasure_pattern
                    ) throws JMSException {
        // Compose message
        requestMapMessage.setBytes("erasure_pattern" , erasure_pattern            ); // byte[] 
        requestMapMessage.setBytes("survival_pattern", survival_pattern           ); // byte[] 
        requestMapMessage.setBytes("cell_length"     , encodingState.encodeLength ); // int
        requestMapMessage.setBytes("survived_cells"  , encodingState.inputs       ); // ByteBuffer[]
        // Set response queue
        requestMapMessage.setJMSReplyTo( queueResponse );
        // Send
        sender.send( requestMapMessage );
    }

    // sendMessage, ByteArrayEncodingState version
    public void sendMessageWithOffset ( 
                       ByteArrayEncodingState encodingState,
                       byte[] survival_pattern,
                       byte[] erasure_pattern
                    ) throws JMSException {
        // Compose message
        requestMapMessage.setBytes("erasure_pattern" , erasure_pattern            ); // byte[] 
        requestMapMessage.setBytes("survival_pattern", survival_pattern           ); // byte[] 
        requestMapMessage.setBytes("cell_length"     , encodingState.encodeLength ); // int
        requestMapMessage.setBytes("survived_cells"  , encodingState.inputs       ); // byte[][]
        // Set response queue
        requestMapMessage.setJMSReplyTo( queueResponse );
        // Send
        sender.send( requestMapMessage );
    }

    // receiveMessage, ByteBufferEncodingState version
    public void receiveMessageReply( ByteBufferEncodingState encodingState ) throws JMSException {
        BytesMessage replyBytesMessage = replyBytesMessage = (BytesMessage) receiver.receive();
        byte [] byteReplyData = new byte[(int) replyBytesMessage.getBodyLength()];
        replyBytesMessage.readBytes( byteReplyData );
        // TODO: check this
        // Must only copy <encodingState.encodeLength> bytes
        // encodingState.outputs = new ByteBuffer(byteReplyData);
    }

    // receiveMessage, ByteArrayEncodingState version
    public void receiveMessageReplyWithOffsets ( ByteArrayEncodingState encodingState ) throws JMSException {
        // TBD
    }
    

}

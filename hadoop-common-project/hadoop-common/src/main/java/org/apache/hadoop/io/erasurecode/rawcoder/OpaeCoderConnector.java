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

// import java.util.Arrays;

// TODO: refactor this for directly RS configurations, ideally from a dynamic config file

/**
 *  Most of code imported from VFProxy.MockVFProxyClient
 */

public class OpaeCoderConnector {

    // TODO: export hard-coded parameters here, ideally in a dynamic config file
    private static String jndiFactoryInital = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private static String jndiProviderURL = "tcp://127.0.0.1:61616";
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

    // Mock-mode test
    private final boolean useMockMode;
    // Mock-mode message buffer
    Hashtable<String, byte[]> messageMockHashtable;

    // Spawn a new Connection and Session
    // NOTE: can't seem to test deploylevel complexity JMS(ActiveMQ) in JUnit, so just use some mock logic during testing.
    //  This is a dirty workaround, **meant to be removed in the future**. 
    //  This current solution requires snippets of code below to be be tested externally.
    //  W.r.t. to overhead in deplopy: since useMockMode is "final", JIT should take care of removing the mock code on 
    //  construction.
    public OpaeCoderConnector () throws NamingException, JMSException {
        // Init mock mode flag based on environment variable 
        if ( System.getenv("HADOOP_MAVEN_TEST_USE_MOCK") != null ) {
            useMockMode = true;
        } else {
            useMockMode = false;
        }

        // If not in mock mode
        if ( !useMockMode ) {
            // Select a VF randomly
            Random random = new Random();
            String sbdfString = PCIesbdfArray [ random.nextInt(PCIesbdfArray.length) ];
            // Connect to provider
            Hashtable <String, String> providerHashtable = new Hashtable <String, String>();
            providerHashtable.put("java.naming.factory.initial", jndiFactoryInital );
            providerHashtable.put("java.naming.provider.url"   , jndiProviderURL   );

            // Queues names
            providerHashtable.put("queue.Request"  + sbdfString, "Request"  + sbdfString );
            providerHashtable.put("queue.Response" + sbdfString, "Response" + sbdfString );
            
            // Create context
            Context context = new InitialContext ( providerHashtable );
            // Lookup queues
            queueRequest  = (Queue) context.lookup("Request"  + sbdfString );
            queueResponse = (Queue) context.lookup("Response" + sbdfString );
            // Lookup (Factory) and start Connection
            QueueConnectionFactory connectionFactory = (QueueConnectionFactory)context.lookup("QueueConnectionFactory");
            QueueConnection connection = connectionFactory.createQueueConnection();
            connection.start();
            
            // Create single-threaded session
            // TODO: also consider tarnsacted Sessions, e.g.: session = connection.createQueueSession(true, 0);
            session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            requestMapMessage = session.createMapMessage();
            
            // Setup Sender for queueRequest
            sender = session.createSender(queueRequest);
            // Setup Receiver for queueResponse
            receiver = session.createReceiver(queueResponse);
        } // !useMockMode
        else { // useMockMode
            messageMockHashtable = new Hashtable<String, byte[]> ();
        } // !useMockMode
    }

    // sendMessage, ByteBufferEncodingState version
    public void sendMessageByteBuffer ( 
                       ByteBufferEncodingState encodingState,
                       byte []                 survival_pattern,
                       byte []                 erasure_pattern
                    ) throws JMSException {
        // Just convert ByteBufferEncodingState to ByteArrayEncodingState
        sendMessageByteArray (
                        encodingState.convertToByteArrayState(), 
                        erasure_pattern,
                        survival_pattern
                    );
    }

    // sendMessage, ByteArrayEncodingState version
    public void sendMessageByteArray ( 
                       ByteArrayEncodingState encodingState,
                       byte []                survival_pattern,
                       byte []                erasure_pattern
                    ) throws JMSException {
        // call explicit parameters version
        sendMessageByteArray ( 
                       encodingState.inputs,
                       encodingState.encodeLength,
                       survival_pattern,
                       erasure_pattern
                    );
    }

    // sendMessage, ByteArrayDecodingState version
    public void sendMessageByteArray ( 
                       ByteArrayDecodingState decodingState,
                       byte [][]              realInputs,
                       int  []                realInputOffsets, // TODO
                       byte []                survival_pattern,
                       byte []                erasure_pattern
                    ) throws JMSException {
        // call explicit parameters version
        sendMessageByteArray ( 
                       realInputs,
                       decodingState.decodeLength,
                       survival_pattern,
                       erasure_pattern
                    );
    }

    // explicit parameters version
    public void sendMessageByteArray ( 
                       byte [][] inputs,
                       int       bufferLength,
                       byte []   survival_pattern,
                       byte []   erasure_pattern
                    ) throws JMSException {
        
        // Serialize input data
        int numDataUnits = inputs.length;
        byte[] serializedByteArrayInputs = new byte [ numDataUnits * bufferLength ];
        serializeBufferArrays ( 
                        inputs,                     // Source
                        serializedByteArrayInputs,  // Dest
                        numDataUnits,               // numBuffers
                        bufferLength                // Length of buffers
                    );
        
        // If not in mock-mode
        if ( !useMockMode ){
            // Compose message
            requestMapMessage.setBytes("erasure_pattern" , erasure_pattern            ); // byte[] 
            requestMapMessage.setBytes("survival_pattern", survival_pattern           ); // byte[] 
            requestMapMessage.setInt  ("cell_length"     , bufferLength               ); // int
            requestMapMessage.setBytes("survived_cells"  , serializedByteArrayInputs  ); // byte[]
            // Set response queue
            requestMapMessage.setJMSReplyTo( queueResponse );
            // Send
            sender.send( requestMapMessage );
        } // !useMockMode
        else { // useMockMode
            // Just mock message passing by local Hashmap
            messageMockHashtable.put("survived_cells"  , serializedByteArrayInputs  ); // byte[]
        } // useMockMode
    }

    // receiveMessage, ByteArrayEncodingState version
    public void receiveMessageReplyByteArray ( ByteArrayEncodingState encodingState ) throws JMSException {
        // call explicit parameters version
        receiveMessageReplyByteArray ( 
                                    encodingState.outputs,
                                    encodingState.encodeLength
                                );
    }

    // receiveMessage, ByteArrayDecodingState version
    public void receiveMessageReplyByteArray ( ByteArrayDecodingState decodingState ) throws JMSException {
        // call explicit parameters version
        receiveMessageReplyByteArray ( 
                                    decodingState.outputs,
                                    decodingState.decodeLength
                                );
    }

    // call explicit parameters version
    public void receiveMessageReplyByteArray ( 
                                    byte[][] outputs,
                                    int      bufferLength
                                ) throws JMSException {
        // Allocate buffer for serialized data
        int numOutputs = outputs.length;
        byte [] byteReplyData = new byte[(int) (numOutputs * bufferLength)];
        
        // If not in mock-mode
        if ( !useMockMode ){
            BytesMessage replyBytesMessage = replyBytesMessage = (BytesMessage) receiver.receive();
            replyBytesMessage.readBytes( byteReplyData );
        } // !useMockMode
        else { // useMockMode
            // Just mock message passing by local Hashmap
            byteReplyData = messageMockHashtable.get("survived_cells"); // byte[]
        } // useMockMode

        // Deserialize output data
        // Just loopback to outputs for mock-mode
        deserializeBufferArrays ( 
                                byteReplyData,  // Source
                                outputs,        // Dest
                                outputs.length, // Num buffers
                                bufferLength    // Length of single buffer
                            );
    }

    // Serialize data from byte[][] to byte[]
    public void serializeBufferArrays ( 
                                    byte [][]   sourceByteArray,
                                    byte []     destByteArray,
                                    int         numBuffers,
                                    int         bufferLength
                                ) {
        for ( int i = 0; i < numBuffers; i++ ) {
            for ( int j = 0; j < bufferLength; j++ ) {
                destByteArray[ (i * bufferLength ) + j ] = sourceByteArray[i][j];
            }
        }
    }

    // Deserialize data from byte[] to byte[][]
    public void deserializeBufferArrays ( 
                                    byte []     sourceByteArray,
                                    byte [][]   destByteArray,
                                    int         numBuffers,
                                    int         bufferLength
                                ) {
        for ( int i = 0; i < numBuffers; i++ ) {
            for ( int j = 0; j < bufferLength; j++ ) {
                destByteArray[i][j] = sourceByteArray[ (i * bufferLength ) + j ];
            }
        }
    }

    // receiveMessage, ByteBufferEncodingState version
    public void receiveMessageReplyByteBuffer( ByteBufferEncodingState encodingState ) throws JMSException {
        // NOTE: This is really unefficient!
        ByteArrayEncodingState byteArrayEncodingState = encodingState.convertToByteArrayState();
        // Just convert to ByteArrayEncodingState
        receiveMessageReplyByteArray ( byteArrayEncodingState );
        // and back to ByteBufferEncodingState
        encodingState = byteArrayEncodingState.convertToByteBufferState(); 
    }
}

package org.apache.hadoop.io.erasurecode.rawcoder;

import java.util.Random;
import java.util.Hashtable;
import java.util.Arrays;
import org.apache.commons.lang3.NotImplementedException;
import java.lang.IllegalStateException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

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

// Logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.erasurecode.rawcoder.OpaeVFPClientConfiguration;

// Import vfproxy
import vfproxy.VFProxyClient;

// Spawn a new Connection and Session
// NOTE: It is not possible to perform integration testing of JMS(ActiveMQ) in JUnit, 
//  so just use some mock logic during unit testing.
//  This is a dirty workaround, **meant to be removed in the future**. 
//  This current solution requires snippets of code below to be be tested externally.
public class OpaeCoderConnector {
    private static final Logger LOG = LoggerFactory.getLogger(OpaeRSRawDecoder.class);

    // RS[K:P] configurations
    private final int numDataUnits;
    private final int numParityUnits;
    // Default values
    private static final int numDataUnitsDefault = 3;
    private static final int numParityUnitsDefault = 2;

    // Mock-mode test
    private final boolean useMockMode;

    /////////////////////////////////////////////
    // VFP Client and configuration parameters //
    /////////////////////////////////////////////
    // VFP Client object
    private VFProxyClient vfpClient = null;
    // Available VFs addresses
    private static String [] sbdfArray;
    // Message timeout
    private final int receiveTimeoutMillisec;
    private final boolean persistentMode;
    private final int msgPriority;
    private final int msgTimeToLiveMillisec;
    // Default values
    private final int receiveTimeoutMillisecDefault = 10000; // 10s
    private final boolean persistentModeDefault = true;
    private final int msgPriorityModeDefault = 9; // Max
    private final int msgTimeToLiveMillisecDefault = 1000; // 10s
    // Key strings for configurations
    public final String sbdfArrayConfString = "dfs.datanode.vfp-client.sbdf-array";
    public final String receiveTimeoutMillisecConfString = "dfs.datanode.vfp-client.jms.receiveTimeoutMillisec";
    public final String persistentModeConfString = "dfs.datanode.vfp-client.jms.persistentMode";
    public final String msgPriorityConfString = "dfs.datanode.vfp-client.jms.msgPriority";
    public final String msgTimeToLiveMillisecConfString = "dfs.datanode.vfp-client.jms.msgTimeToLiveMillisec";

    // No-args constructor
    public OpaeCoderConnector () throws NamingException, JMSException, IllegalStateException {
        // Use defaults
        this (
                numDataUnitsDefault,
                numParityUnitsDefault
            );
    }

    // Constructor with arguments
    public OpaeCoderConnector (
                            int rsK,
                            int rsP
                        ) throws NamingException, JMSException, IllegalStateException {

        // Init attributes
        numDataUnits   = rsK;
        numParityUnits = rsP;

        ///////////////////////////////////////
        // Get parameters from configuration //
        ///////////////////////////////////////
        OpaeVFPClientConfiguration conf = new OpaeVFPClientConfiguration( true );
        sbdfArray = conf.getStrings(sbdfArrayConfString);
        if ( sbdfArray == null ) {
            throw new IllegalStateException("Parsed sbdfArray is null");
        }
        receiveTimeoutMillisec = conf.getInt(receiveTimeoutMillisecConfString, receiveTimeoutMillisecDefault);
        persistentMode         = conf.getBoolean(persistentModeConfString, persistentModeDefault);
        msgPriority            = conf.getInt(msgPriorityConfString, msgPriorityModeDefault);
        msgTimeToLiveMillisec  = conf.getInt(msgTimeToLiveMillisecConfString, msgTimeToLiveMillisecDefault);

        ////////////////////////
        // Mock mode handling //
        ////////////////////////
        // Init mock mode flag based on environment variable 
        if ( System.getenv("HADOOP_MAVEN_TEST_USE_MOCK") != null ) {
            useMockMode = true;
        } else {
            useMockMode = false;
        }

        // If not in mock mode
        if ( !useMockMode ) {
            // Instantiate VFP Client
            vfpClient = new VFProxyClient (
                                        rsK,
                                        rsP,
                                        sbdfArray,
                                        receiveTimeoutMillisec,
                                        persistentMode,
                                        msgPriority,
                                        msgTimeToLiveMillisec
                                    );
        } // !useMockMode
    }

    // Raw byte[] version
    // TODO: figure out input/outputOffsets
    // - Wrap VFProxyClient.callToVFProxy()
    // - Add mock logic for JUnit testing
    public byte [] callToVFProxy ( 
                            int erasurePattern,
                            int survivalPattern,
                            int bufferLength,
                            byte[] survivedCells
                    ) throws JMSException, TimeoutException {
        // Return data
        byte[] byteReplyData = null;

        // If not in mock-mode
        if ( !useMockMode ){
            // Call to VFP
            byteReplyData = vfpClient.callToVFProxy (
                                        erasurePattern,
                                        survivalPattern,
                                        bufferLength,
                                        survivedCells
                                    );       
        } // !useMockMode
        else { // useMockMode
            // Just loop back
            // NOTE: this works under the assumption of numParityUnits <= numDataUnits,
            //          which always holds any EC in general
            byteReplyData = Arrays.copyOf(survivedCells, bufferLength * numParityUnits);
        } // useMockMode

        return byteReplyData;

    }

    // ByteArrayEncodingState version, wraps raw byte[] version
    public void callToVFProxy ( 
                            int erasurePattern,
                            int survivalPattern,
                            ByteBufferEncodingState encodingState
                        ) throws JMSException, TimeoutException {
        // Convert to ByteArray
        // ByteArrayEncodingState byteArrayEncodingState = encodingState.convertToByteArrayState();
        // Serialize input data
        int bufferLength = encodingState.encodeLength;
        byte[] serializedByteArrayInputs = new byte [ numDataUnits * bufferLength ];
        int i = 0;
        for ( ByteBuffer bb : encodingState.outputs ) {
            int offset = (i++) * bufferLength;
            // bb.rewind(); // test
            bb.get(
                    serializedByteArrayInputs,  // byte[] dst
                    offset,                     // int offset
                    bufferLength                // int length
                );
        }
        // serializeArrayOfArraysToArray ( 
        //                 byteArrayEncodingState.inputs,  // Source   byte [][]
        //                 serializedByteArrayInputs,      // Dest     byte []
        //                 numDataUnits,                   // numBuffers
        //                 bufferLength                    // Length of buffers
        //             );

        // Call raw version
        byte [] byteReplyData = callToVFProxy ( 
                        erasurePattern,
                        survivalPattern,
                        bufferLength,
                        serializedByteArrayInputs
                    );

        // Deserialize output data
        // deserializeArrayToArrayOfArrays ( 
        //     byteReplyData,                  // Source     byte [] 
        //     byteArrayEncodingState.outputs, // Dest       byte [][]
        //     encodingState.outputs.length,   // Num buffers
        //     bufferLength                    // Length of single buffer
        // );
        // Re-convert into ByteBuffer
        i = 0;
        LOG.warn("bufferLength " + bufferLength);
        for ( ByteBuffer bb : encodingState.outputs ) {
            int offset = (i++) * bufferLength;
            LOG.warn("offset " + offset);
            LOG.warn("bb.limit() " + bb.limit());
            LOG.warn("bb.capacity() " + bb.capacity());
            bb.rewind();
            bb.put(
                byteReplyData,  // byte[] src
                offset,         // int offset
                bb.limit()      // int length
            );
        }
    }

    // TODO: figure out outputOffsets
    // ByteArray version, wraps raw byte[] version
    public void callToVFProxy ( 
                            int erasurePattern,
                            int survivalPattern,
                            ByteArrayEncodingState encodingState
                        ) throws JMSException, TimeoutException {
        // Return data
        byte [] byteReplyData = null;

        // Serialize input data
        int bufferLength = encodingState.encodeLength;
        byte[] serializedByteArrayInputs = new byte [ numDataUnits * bufferLength ];
        serializeArrayOfArraysToArray ( 
                        encodingState.inputs,       // Source byte [][]
                        serializedByteArrayInputs,  // Dest   byte []
                        numDataUnits,               // numBuffers
                        bufferLength                // Length of buffers
                    );

        // Call raw version
        byteReplyData = callToVFProxy ( 
                        erasurePattern,
                        survivalPattern,
                        bufferLength,
                        serializedByteArrayInputs
                    );

        // Deserialize output data
        deserializeArrayToArrayOfArrays ( 
                                byteReplyData,                // Source byte []
                                encodingState.outputs,        // Dest   byte [][]
                                encodingState.outputs.length, // Num buffers
                                bufferLength                  // Length of single buffer
                            );        
    }

    //////////////
    // Ulitites //
    //////////////
    // Serialize data from byte[][] to byte[]
    private void serializeArrayOfArraysToArray ( 
                                    byte [][]   sourceByteArray,
                                    byte []     destByteArray,
                                    int         numBuffers,
                                    int         bufferLength
                                ) {
        for ( int i = 0; i < numBuffers; i++ ) {
            // Copy bufferLength bytes
            System.arraycopy (
                // src
                sourceByteArray[i],
                // srcIndex
                0,
                // dest
                destByteArray,
                // destIndex
                i * bufferLength,
                // len
                bufferLength
            );
        }
    }

    // Deserialize data from byte[] to byte[][]
    private void deserializeArrayToArrayOfArrays ( 
                                    byte []     sourceByteArray,
                                    byte [][]   destByteArray,
                                    int         numBuffers,
                                    int         bufferLength
                                ) {
        for ( int i = 0; i < numBuffers; i++ ) {
            // Copy bufferLength bytes
            System.arraycopy (
                // src
                sourceByteArray,
                // srcIndex
                i * bufferLength,
                // dest
                destByteArray[i],
                // destIndex
                0,
                // len
                bufferLength
            );
        }
    }

    /////////////
    // Getters //
    /////////////
    public int getNumDataUnits() {
        return numDataUnits;
    }
    public int getNumParityUnits() {
        return numParityUnits;
    }
    public String [] getSbdfArray() {
        return sbdfArray;
    }
    public int getReceiveTimeoutMillisecs() {
        return receiveTimeoutMillisec;
    }
    public boolean getPersistentMode() {
        return persistentMode;
    }
    public int getMsgPriority() {
        return msgPriority;
    }
    public int getTimeToLiveMillisec() {
        return msgTimeToLiveMillisec;
    }
}

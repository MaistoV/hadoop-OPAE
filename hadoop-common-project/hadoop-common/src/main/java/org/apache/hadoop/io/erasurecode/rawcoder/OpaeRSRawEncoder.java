/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.OpaeCoderConnector;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;

// Logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// JMS
import javax.naming.NamingException;
import javax.jms.JMSException;

// Exceptions
import java.util.concurrent.TimeoutException;
import java.io.IOException;
import org.apache.commons.lang3.NotImplementedException;

// TODO: refactor this for directly RS configurations, ideally from a dynamic config file

@InterfaceAudience.Private
public class OpaeRSRawEncoder extends RawErasureEncoder {
  private static final Logger LOG = LoggerFactory.getLogger(OpaeRSRawEncoder.class);

  /**
   * The "encode" methods are inherited by super, i.e. RawErasureEncoder
   */

  // Static of encoding message
  private final int survivalPattern;
  private final int erasurePattern;
  private int fallBackThreshHold;
  private final int fallBackThreshHold_RS_3_2 = 512 * 1024; // 512KB
  private final int fallBackThreshHold_RS_6_3 = 256 * 1024; // 256KB

  // Software encoder for short lengths
  private final RawErasureEncoder softwareEncoder;

  // Connctor to JMS provider
  OpaeCoderConnector localOpaeCoderConnector;

  public OpaeRSRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

    if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException("Invalid numDataUnits and numParityUnits");
    }

    // Check supported codec
    // NOTE: these must only match with available bitstreams, new ones can be generated
    if ( !(
          // 10:4
          // ( (getNumDataUnits() == 10) && (getNumParityUnits() == 4) ) || // Unsupported for now
          // 6:3
          ( (getNumDataUnits() ==  6) && (getNumParityUnits() == 3) ) ||
          // 3:2
          ( (getNumDataUnits() ==  3) && (getNumParityUnits() == 2) )
          )
        ) {
      throw new HadoopIllegalArgumentException(
            "Invalid numDataUnits (" + getNumDataUnits() + ") and numParityUnits (" + getNumParityUnits() + ")"
          );
    }

    // Init thresholds
    // Defaults to 6:3
    fallBackThreshHold = fallBackThreshHold_RS_6_3;
    // 3:2
    if ( (getNumDataUnits() ==  3) && (getNumParityUnits() == 2) ) {
      fallBackThreshHold = fallBackThreshHold_RS_3_2;
    }
    // 6:3
    // else if ( (getNumDataUnits() ==  6) && (getNumParityUnits() == 3) ) {
    //   fallBackThreshHold = fallBackThreshHold_RS_6_3;
    // }
    
    // Check maximum number of units
    // NOTE: this is only limited by the current FPGA implementation,
    // and could esily be extended
    if ( getNumAllUnits() > 16 ) {
      throw new HadoopIllegalArgumentException("numDataUnits + numParityUnits must currently be <= 16");
    }

    // Compose static part of encoding message
    // Init mask
    final int RS_PATTERN_MASK = (1 << getNumAllUnits()) -1;
		// For encoding, these patterns are always the same
		survivalPattern = RS_PATTERN_MASK &  ((1 << getNumDataUnits()) -1); // Bitmask for first k blocks
		erasurePattern	= RS_PATTERN_MASK & ~((1 << getNumDataUnits()) -1); // Bitmask for last p blocks

    // Create new connector
    try {
      localOpaeCoderConnector = new OpaeCoderConnector(
                                                      getNumDataUnits(),    // rsK
                                                      getNumParityUnits()   // rsP
                                                    );
    }
    catch ( NamingException e ) {
      e.printStackTrace();
      throw new HadoopIllegalArgumentException ("Caught NamingException during connection" + e);
    }
    catch ( JMSException e ) {
      e.printStackTrace();
      throw new HadoopIllegalArgumentException ("Caught JMSException during connection" + e);
    }

    // Also create a software (native) coder for delegating encoding of short-buffers
    RawErasureCoderFactory coderFactory = null;
    if ( ErasureCodeNative.isNativeCodeLoaded() ) {
      coderFactory = new NativeRSRawErasureCoderFactory();
    }
    else {
      coderFactory = new RSRawErasureCoderFactory();
    }
    softwareEncoder = coderFactory.createEncoder(coderOptions);
  }

  // Check if encodeLength is safe for VFP offloading
  private boolean isSafeCodingState ( EncodingState encodingState ) {
    LOG.info("[isSafeCodingState]  encodingState.encodeLength: " + encodingState.encodeLength );
    return ((encodingState.encodeLength % 64) == 0 ) &&
            ( encodingState.encodeLength >= fallBackThreshHold ) ;
  }

  @Override
  protected void doEncode(ByteBufferEncodingState encodingState) throws IOException {
    LOG.info("Entering doEncode<ByteBufferEncodingState>");
    
    // Check args
    if ( ! isSafeCodingState(encodingState) ) {
      LOG.warn("[doEncode] Short encodingState.encodeLength " + encodingState.encodeLength +
          " delegating to " + softwareEncoder.getClass().getName() 
        );
      // throw new HadoopIllegalArgumentException("Encode length " + encodingState.encodeLength + " should be integer multiple of 64" );
      // Delegate to software encoder 
      softwareEncoder.doEncode(encodingState);
      return;
    }
    // throw new NotImplementedException();
  
    // Reset outputs
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.encodeLength);

    try {
    LOG.info("[doEncode] Calling callToVFProxy");
      // Send new message and wait for response
      localOpaeCoderConnector.callToVFProxy (
                                erasurePattern,
                                survivalPattern,
                                encodingState // ByteBufferEncodingState
                              );
    }
    catch ( JMSException e ) {
      throw new IOException("Caught JMSException during encoding " + e);
    }
    catch ( TimeoutException e ) {
      throw new IOException("Caught TimeoutException during encoding " + e);
    }
  }

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) throws IOException {
    LOG.info("Entering doEncode<ByteArrayEncodingState>");
    
    // Check args
    if ( ! isSafeCodingState(encodingState) ) {
      LOG.warn("[doEncode] Short encodingState.encodeLength " + encodingState.encodeLength +
          " delegating to " + softwareEncoder.getClass().getName() 
        );
      // throw new HadoopIllegalArgumentException("Encode length " + encodingState.encodeLength + " should be integer multiple of 64" );
      // Delegate to software encoder 
      softwareEncoder.doEncode(encodingState);
      return;
    }

    ///////////////////////////////
    // Otherwise encode with VFP //
    ///////////////////////////////

    // Reset outputs
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.outputOffsets,
        encodingState.encodeLength);
        
    try {
      // Send new message and wait for response
      LOG.info("[doEncode] Calling callToVFProxy");

      localOpaeCoderConnector.callToVFProxy (
                                erasurePattern,
                                survivalPattern,
                                encodingState // ByteArrayEncodingState
                              );
    }
    catch ( JMSException e ) {
      throw new IOException("Caught JMSException during encoding " + e);
    }
    catch ( TimeoutException e ) {
      throw new IOException("Caught TimeoutException during encoding " + e);
    }
  }

  // @Override
  // NOTE: "DirectBuffer" refers to ByteBuffer
  public boolean preferDirectBuffer() {
      // TODO: revert to true once implemented callToVFProxy for ByteBuffer
      return true;
      // return false;
  }
}

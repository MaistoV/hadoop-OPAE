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
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.OpaeCoderConnector;
import org.apache.commons.lang3.NotImplementedException;

// Logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.naming.NamingException;
import javax.jms.JMSException;
import java.io.IOException;

// TODO: refactor this for directly RS configurations, ideally from a dynamic config file

@InterfaceAudience.Private
public class OpaeRSRawDecoder extends RawErasureDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(OpaeRSRawDecoder.class);

  private int[] cachedErasedIndexes;
  private int[] validIndexes;
  private int numErasedDataUnits;
  private boolean[] erasureFlags;

  // This is constant across calls, for a single EC policy
  private final int RS_PATTERN_MASK;

  // Connctor to JMS provider
  OpaeCoderConnector localOpaeCoderConnector;
  
  /**
   * The "decode" methods are inherited by super, i.e. RawErasureDecoder
   */

  public OpaeRSRawDecoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

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
        throw new HadoopIllegalArgumentException("Invalid numDataUnits (" + getNumDataUnits() + ") and numParityUnits (" + getNumParityUnits() + ")");

    }

    // Check maximum number of units
    // NOTE: this is only limited by the current FPGA implementation,
    // and could esily be extended
    if ( getNumAllUnits() > 16 ) {
      throw new HadoopIllegalArgumentException(
          "numDataUnits + numParityUnits must curretly be <= 16");
    }

    // Compose static part of encoding message
    // Init mask
    RS_PATTERN_MASK = (1 << getNumAllUnits()) -1;

    // Create new connector
    localOpaeCoderConnector = null;
    try {
      int oneMB = 1048576; // 1024 * 1024;
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
  }

  // Convert an array of integers into a packed bitmask
  public int intArrayToUint16 ( int[] intArray ) {
      // Reset output
      int ret = 0;

      // Set the target bits
      for ( int i = 0; (i < intArray.length) && (i < getNumAllUnits()); i++ ){
        ret |= 1 << (intArray[i]);
      }

      // Mask and return
      return (ret & RS_PATTERN_MASK);
  }

  @Override
  protected void doDecode(ByteBufferDecodingState decodingState) {
    LOG.info("Entering doDecode<ByteBufferDecodingState>");
    // CoderUtil.resetOutputBuffers(decodingState.outputs,
    //     decodingState.decodeLength);
      LOG.info("[doDecode] Calling callToVFProxy");
      // LOG.info("[doDecode]  erasurePattern  0x" + String.format("%04x", erasurePattern   ) );
      // LOG.info("[doDecode]  survivalPattern  0x" + String.format("%04x", survivalPattern   ));
      LOG.info("[doDecode]  encodingState.encodeLength: " + decodingState.decodeLength );
    throw new NotImplementedException();
  }

  @Override
  protected void doDecode(ByteArrayDecodingState decodingState) throws IOException {
    LOG.info("Entering doDecode<ByteArrayDecodingState>");
    throw new NotImplementedException();
    
    // int dataLen = decodingState.decodeLength;
    // CoderUtil.resetOutputBuffers(decodingState.outputs,
    //     decodingState.outputOffsets, dataLen);

    // // Init validIndexes by picking up non-null values
    // prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);

    // // Compose erasure and survival patterns
		// int erasurePattern  = intArrayToUint16 ( decodingState.erasedIndexes );
		// int survivalPattern	= intArrayToUint16 ( validIndexes                ); 

    // // Prepare inputs
    // byte[][] realInputs = new byte[getNumDataUnits()][];
    // int[] realInputOffsets = new int[getNumDataUnits()];
    // for (int i = 0; i < getNumDataUnits(); i++) {
    //   realInputs[i] = decodingState.inputs[validIndexes[i]];
    //   realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
    // }
    
    // // TODO: combine realInputs + realInputsOffsets -> realInputs
    // // NOTE: refere to RSUtil.java:encodeData()
    // // NOTE: refere to jni_rs_decoder.c -> jni_common.c:getInputs()
    // // TBD

    // // Call to decode API
    // try {
    //   // Send new message and wait for response
    //   localOpaeCoderConnector.callToVFProxy (
    //                             erasurePattern,
    //                             survivalPattern,
    //                             realInputs // byte [] survivedCells
    //                           );
    // }
    // catch ( JMSException e ) {
    //   throw new IOException("Caught JMSException during decoding" + e);
    // }
    
  }

  private <T> void prepareDecoding(T[] inputs, int[] erasedIndexes) {
    int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
    if (Arrays.equals(this.cachedErasedIndexes, erasedIndexes) &&
        Arrays.equals(this.validIndexes, tmpValidIndexes)) {
      return; // Optimization. Nothing to do
    }
    this.cachedErasedIndexes =
            Arrays.copyOf(erasedIndexes, erasedIndexes.length);
    this.validIndexes =
            Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);
  }
}

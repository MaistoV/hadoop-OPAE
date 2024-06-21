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

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.naming.NamingException;
import javax.jms.JMSException;
import java.io.IOException;

// TODO: refactor this for directly RS configurations, ideally from a dynamic config file

@InterfaceAudience.Private
public class OpaeRSRawDecoder extends RawErasureDecoder {
  private int[] cachedErasedIndexes;
  private int[] validIndexes;
  private int numErasedDataUnits;
  private boolean[] erasureFlags;

  // This is constant across calls, for a single EC policy
  private final int RS_PATTERN_MASK;
  private byte[] survival_pattern;
  private byte[] erasure_pattern ;

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
          ( (getNumDataUnits() == 10) && (getNumParityUnits() == 4) ) ||
          // 6:3
          ( (getNumDataUnits() ==  6) && (getNumParityUnits() == 3) ) ||
          // 3:2
          ( (getNumDataUnits() ==  3) && (getNumParityUnits() == 2) )
          )
        ) {
      throw new HadoopIllegalArgumentException(
          "Invalid numDataUnits and numParityUnits");
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
    // Allocate space for erasure and survival patterns
    // NOTE: this relies on getNumAllUnits() < 16
    survival_pattern = new byte[2]; // 16 bits
    erasure_pattern  = new byte[2]; // 16 bits

    // Create new connector
    localOpaeCoderConnector = null;
    try {
      localOpaeCoderConnector = new OpaeCoderConnector();
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
    // CoderUtil.resetOutputBuffers(decodingState.outputs,
    //     decodingState.decodeLength);
  }

  @Override
  protected void doDecode(ByteArrayDecodingState decodingState) throws IOException {
    int dataLen = decodingState.decodeLength;
    CoderUtil.resetOutputBuffers(decodingState.outputs,
        decodingState.outputOffsets, dataLen);

    // Init validIndexes by picking up non-null values
    prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);

    // Compose erasure and survival patterns
    // First compose as int
		int erasure_pattern_int = intArrayToUint16 ( decodingState.erasedIndexes );
		int survived_cells_int	= intArrayToUint16 ( validIndexes                ); 

    // Then, split into bytes
    survival_pattern[0] = (byte) (survived_cells_int  % 16);
    erasure_pattern [0] = (byte) (erasure_pattern_int % 16);
    survival_pattern[1] = (byte) (survived_cells_int  / 16);
    erasure_pattern [1] = (byte) (erasure_pattern_int / 16);

    // Prepare inputs
    byte[][] realInputs = new byte[getNumDataUnits()][];
    int[] realInputOffsets = new int[getNumDataUnits()];
    for (int i = 0; i < getNumDataUnits(); i++) {
      realInputs[i] = decodingState.inputs[validIndexes[i]];
      realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
    }

    // Call to decode API
    try {
      // Send new message
      localOpaeCoderConnector.sendMessageByteArray (
                                decodingState,
                                realInputs,
                                realInputOffsets,
                                erasure_pattern,
                                survival_pattern
                              );

      // Wait for response
      localOpaeCoderConnector.receiveMessageReplyByteArray ( decodingState );
    }
    catch ( JMSException e ) {
      throw new IOException("Caught JMSException during decoding" + e);
    }
    
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

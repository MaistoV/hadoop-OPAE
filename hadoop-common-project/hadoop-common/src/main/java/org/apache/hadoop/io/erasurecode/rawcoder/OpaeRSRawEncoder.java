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
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.OpaeCoderConnector;

import javax.naming.NamingException;
import javax.jms.JMSException;

// TODO: refactor this for directly RS configurations, ideally from a dynamic config file
// TODO: handle exceptions, in particular: NamingException and JMSException

@InterfaceAudience.Private
public class OpaeRSRawEncoder extends RawErasureEncoder {

  /**
   * The "encode" methods are inherited by super, i.e. RawErasureEncoder
   */

  // Static of encoding message
  byte[] survival_pattern;
  byte[] erasure_pattern;

  // Connctor to JMS provider
  OpaeCoderConnector localOpaeCoderConnector;

  public OpaeRSRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

    if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException(
          "Invalid numDataUnits and numParityUnits");
    }

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
    int RS_PATTERN_MASK = (1 << getNumAllUnits()) -1;
    // NOTE: this relies on getNumAllUnits() < 16
    survival_pattern = new byte[2]; // 16 bits
    erasure_pattern  = new byte[2]; // 16 bits
		// For encoding, these patterns are always the same
    // First compose as int
		int survived_cells_int  = RS_PATTERN_MASK &  ((1 << getNumDataUnits()) -1); // Bitmask for first k blocks
		int erasure_pattern_int	= RS_PATTERN_MASK & ~((1 << getNumDataUnits()) -1); // Bitmask for last p blocks
    // Then, split into bytes
    survival_pattern[0] = (byte) (survived_cells_int  % 16);
    erasure_pattern [0] = (byte) (erasure_pattern_int % 16);
    survival_pattern[1] = (byte) (survived_cells_int  / 16);
    erasure_pattern [1] = (byte) (erasure_pattern_int / 16);

    // Create new connector
    try {
      localOpaeCoderConnector = new OpaeCoderConnector(); 
    }
    catch ( NamingException e ) {
      e.printStackTrace();
      throw new HadoopIllegalArgumentException (
          "Encountered unexpected NamingException");
    }
    catch ( JMSException e ) {
      e.printStackTrace();
      throw new HadoopIllegalArgumentException (
          "Encountered unexpected JMSException");
    }
  }

  @Override
  protected void doEncode(ByteBufferEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.encodeLength);

    try {
      // Send new message
      localOpaeCoderConnector.sendMessageByteBuffer (
                                encodingState, 
                                erasure_pattern,
                                survival_pattern
                              );

      // Wait for response
      localOpaeCoderConnector.receiveMessageReplyByteBuffer ( encodingState );
    }
    catch ( JMSException e ) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  @Override
  // TODO: figure out the difference w.r.t. WithOffsets
  protected void doEncode(ByteArrayEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.outputOffsets,
        encodingState.encodeLength);

    try {
      // Send new message
      localOpaeCoderConnector.sendMessageByteArray (
                                encodingState,
                                erasure_pattern,
                                survival_pattern
                              );

      // Wait for response
      localOpaeCoderConnector.receiveMessageReplyByteArray ( encodingState );
    }
    catch ( JMSException e ) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  // TODO: check this
  // @Override
  public boolean preferDirectBuffer() {
      return true;
  }
}

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

import org.apache.hadoop.io.erasurecode.rawcoder.OpaeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.OpaeRSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.OpaeRSRawDecoder;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * Test CodecRegistry.
 */
public class TestCodecRegistry {
  @Test
  public void testVFProxyResponseByteBuffer() {
    // Get encoder from Factory
    int numDataUnits   = 3;
    int numParityUnits = 2;
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    OpaeRSRawErasureCoderFactory coderFactory = new OpaeRSRawErasureCoderFactory();
    OpaeRSRawEncoder encoder = coderFactory.createEncoder();

    // Encode (Send message, wait for reply)
    ByteBuffer[] inputs =  new byte [numDataUnits  ][length];
    ByteBuffer[] outputs = new byte [numParityUnits][length];
    encoder.encode( inputs, outputs );

    // Check return data (loopback for now)
    assertEquals(inputs[0], outouts[0]);
    assertEquals(inputs[1], outouts[1]);
  }

  // @Test
  // public void testVFProxyResponseByteArray() {
  //   // TBD
  //   // byte[][] inputs =  new byte [ numDataUnits   ] [ length ];
  //   // byte[][] outputs = new byte [ numParityUnits ] [ length ];
  // }
}

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
import org.apache.hadoop.io.erasurecode.rawcoder.OpaeCoderConnector;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.nio.ByteBuffer;
import java.util.Random;

import javax.naming.NamingException;
import javax.jms.JMSException;
import java.io.IOException;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Test preliminary OpaeRSCoding feature.
 */
public class TestOpaeRSRawEncoder {

  @Test
  public void testNewOpaeRSRawEncoder() {
    // Get encoder from Factory
    int numDataUnits   = 3;
    int numParityUnits = 2;
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    OpaeRSRawErasureCoderFactory coderFactory = new OpaeRSRawErasureCoderFactory();
    OpaeRSRawEncoder encoder = coderFactory.createEncoder(coderOptions);

    // Assert on class
    assertNotNull( encoder );
  }

  @Test
  public void testEncodeByteArray() {
    // Get encoder from Factory
    int numDataUnits   = 3;
    int numParityUnits = 2;
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    OpaeRSRawErasureCoderFactory coderFactory = new OpaeRSRawErasureCoderFactory();
    OpaeRSRawEncoder encoder = coderFactory.createEncoder(coderOptions);

    // Declare buffers
    int length = 8;
    byte[][] inputs =  new byte [numDataUnits  ][length];
    byte[][] outputs = new byte [numParityUnits][length];

    // init output
    for ( byte [] output : outputs ) {
      for ( int i = 0; i < output.length; i++ ) {
          output[i] = (byte)255;
      }
    }

    // Compose message
    Random random = new Random();
    for ( byte [] input : inputs ) {
        random.nextBytes(input);
    }

    // Encode (Send message, wait for reply)
    try {
      encoder.encode( inputs, outputs );
    }
    catch ( IOException e ) {
      fail("Encountered unexpected exception");
    }

    // Check return data (loopback for now)
    assertArrayEquals(inputs[0], outputs[0]);
    assertArrayEquals(inputs[1], outputs[1]);
  }

  @Test
  public void testEncodeByteBuffer() {
    // Get encoder from Factory
    int numDataUnits   = 3;
    int numParityUnits = 2;
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    OpaeRSRawErasureCoderFactory coderFactory = new OpaeRSRawErasureCoderFactory();
    OpaeRSRawEncoder encoder = coderFactory.createEncoder(coderOptions);

    // Declare buffers
    int length = 8;
    // ByteBuffer[] inputs =  new ByteBuffer() [numDataUnits  ];
    // ByteBuffer[] outputs = new ByteBuffer() [numParityUnits];

    // TODO: implement
    fail("yet unimplemented");
  }

}

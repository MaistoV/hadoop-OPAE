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
import java.util.Arrays;

import javax.naming.NamingException;
import javax.jms.JMSException;
import java.io.IOException;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Test preliminary OpaeRSCoding feature.
 */
public class TestOpaeRSRawDecoder {

  private final int numDataUnits   = 3;
  private final int numParityUnits = 2;
  private final int numAllUnits = numDataUnits + numParityUnits;
  private ErasureCoderOptions coderOptions;
  private OpaeRSRawErasureCoderFactory coderFactory;
  private OpaeRSRawDecoder decoder;
  private final int length = 8;
  private byte[][] inputs;
  private byte[][] outputs;


  @Before 
  public void initDecoder()  {
    // Get encoder from Factory
    coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    coderFactory = new OpaeRSRawErasureCoderFactory();
    decoder = coderFactory.createDecoder(coderOptions);
    
    assertNotNull(decoder);

    // Declare buffers
    inputs = new byte [numAllUnits][length];

    // Compose message
    Random random = new Random();
    for ( byte [] input : inputs ) {
        random.nextBytes(input);
    }
    // for ( int i = 0; i < numAllUnits; i++ ) {
    //     for ( int j = 0; j < length; j++ ) {
    //       inputs [i][j] = (byte) i;
    //     }
    // }

  }

  @Test
  public void testNewOpaeRSRawDecoder() {
    // Get decoder from Factory
    coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    coderFactory = new OpaeRSRawErasureCoderFactory();
    decoder = coderFactory.createDecoder(coderOptions);

    // Assert on class
    assertNotNull( decoder );
  }

  @Test
  public void testDecodeByteArray1Erasure() {
    // Testing 1-hot erasure patterns:
    // 00001
    // 00010
    // 00100
    // 01000
    // 10000
    int numErasures = 1;
    outputs = new byte [numErasures][length];

    // Declare an array of indexes
    int numErasurePatterns = numAllUnits;
    int [][] erasedIndexesArary = new int [numErasurePatterns][];
    for ( int i = 0; i < numErasurePatterns; i++ ) {
      // Re-init output
      for ( byte [] output : outputs ) {
        for ( int j = 0; j < output.length; j++ ) {
            output[j] = (byte)255;
        }
      }
      assertFalse(Arrays.equals(inputs[0], outputs[0]));

      // Compose erasure pattern 2^i
      erasedIndexesArary[i] = new int [] {i};

      // Encode (Send message, wait for reply)
      try {
        decoder.decode( inputs, erasedIndexesArary[i], outputs );
      }
      catch ( IOException e ) {
        fail("Encountered unexpected IOException");
      }

      // Check return data (loopback for now)
      assertArrayEquals(inputs[0], outputs[0]);
    }

  }

  @Test
  public void testDecodeByteArray2Erasures() {
    // Testing 2-hot erasure patterns:
    // 00011
    // 00101
    // 00110
    // 01001
    // 01010
    // 01100
    // 10001
    // 10010
    // 10100
    // 11000
    int numErasures = 2;
    outputs = new byte [numErasures][length];
    int numErasurePatterns = 10; // binom(numAllUnits, numErasures)
    // int erasedIndexesArary [numErasurePatterns][numErasures]
    // Count 1s
    int [][] erasedIndexesArary = new int [][] { 
                                               // e: 0, 1
                                                    {0, 1}, // 0: 00011
                                                    {0, 2}, // 1: 00101
                                                    {1, 2}, // 2: 00110
                                                    {0, 3}, // 3: 01001
                                                    {1, 3}, // 4: 01010
                                                    {2, 3}, // 5: 01100
                                                    {0, 4}, // 6: 10001
                                                    {1, 4}, // 7: 10010
                                                    {2, 4}, // 8: 10100
                                                    {3, 4}  // 9: 11000
                                                    };
    // Count 0s
    int [][] validIndexesArary = new int [][] { 
                                               // e: 0, 1, 2
                                                    {2, 3, 4}, // 0: 00011
                                                    {1, 3, 4}, // 1: 00101
                                                    {0, 3, 4}, // 2: 00110
                                                    {1, 2, 4}, // 3: 01001
                                                    {0, 2, 4}, // 4: 01010
                                                    {0, 1, 4}, // 5: 01100
                                                    {1, 2, 3}, // 6: 10001
                                                    {0, 2, 3}, // 7: 10010
                                                    {0, 1, 2}, // 8: 10100
                                                    {0, 1, 2}  // 9: 11000
                                                    };
    // Loop over erasure patterns
    for ( int i = 0; i < numErasurePatterns; i++ ) {
      // Re-init output
      for ( byte [] output : outputs ) {
        for ( int j = 0; j < output.length; j++ ) {
            output[j] = (byte)255;
        }
      }
      for ( int e = 0; e < numErasures; e++ ) {
        assertFalse(Arrays.equals(inputs[erasedIndexesArary[i][e]], outputs[e]));
      }

      // Re-init inpus
      // NOTE: decoder detects valid inputs by checking for non-null values
      //        therefore, we need to null the survived inputs
      byte [][] effectiveInputs = new byte [numAllUnits][length];
      for ( int j = 0; j < numAllUnits; j++ ) {
        // Copy-in the allt the cells
        effectiveInputs[j] = Arrays.copyOf(inputs[j], length);
      }
      // Null-out the erased ones
      for ( int j = 0; j < numErasures; j++ ) {
        effectiveInputs[erasedIndexesArary[i][j]] = null; 
      }

      // Encode (Send message, wait for reply)
      try {
        decoder.decode( effectiveInputs, erasedIndexesArary[i], outputs );
      }
      catch ( IOException e ) {
        fail("Encountered unexpected IOException");
      }

      // Check return data (loopback for now)
      for ( int e = 0; e < numErasures; e++ ) {
        assertArrayEquals( "[i,e] = [" + i + "," + e + "]\n", 
          effectiveInputs[validIndexesArary[i][e]], outputs[e] );
      }
    }

  }

  @Test
  public void testIntArrayToUint16() {
    // Declare input and output
    int intArray[] = null;
    int pattern = 0;

    // Contiguous pattern 0x1f
    intArray = new int [] { 0, 1, 2, 3, 4 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x1f );

    // Contiguous pattern 0x1f (shuffled)
    intArray = new int [] { 2, 0, 1, 4, 3 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x1f );

    // Contiguous pattern 0x00 (empty array)
    intArray = new int [] {};
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x00 );

    // Alternate pattern = 0x15
    intArray = new int [] { 0, 2, 4 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x15 );

    // Alternate pattern = 0x15 (shuffled)
    intArray = new int [] { 2, 0, 4 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x15 );

    // Alternate pattern = 0x0A
    intArray = new int [] { 1, 3 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x0A );

    // Alternate pattern = 0x0A (shuffled)
    intArray = new int [] { 3, 1 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x0A );

    // Overlong array = 0xff -> 0x1f
    intArray = new int [] { 0, 1, 2, 3, 4, 5, 6, 7 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x001f );

    // Overlong array = 0xff -> 0x1f (shuffled)
    intArray = new int [] { 2, 3, 5, 7, 6, 4, 0, 1 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x001f );

    // Shorter array, 1 bit -> 0x01
    intArray = new int [] {0};
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x01 );

    // Shorter array, 1 bit -> 0x02
    intArray = new int [] {1};
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x02 );

    // Shorter array, 1 bit -> 0x04
    intArray = new int [] {2};
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x04 );

    // Shorter array, 2 bit -> 0x03
    intArray = new int [] { 0, 1 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x03 );

    // Shorter array, 2 bit -> 0x05
    intArray = new int [] { 2, 0 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x05 );
    
    // Shorter array, 3 bit -> 0x07
    intArray = new int [] { 0, 1, 2 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x0b );
    
    // Shorter array, 3 bit -> 0x0b
    intArray = new int [] { 0, 1, 3 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, 0x0b );

    // Null values -> propagate null
    intArray = new int [] { 0, 1, 3 };
    pattern = decoder.intArrayToUint16 ( intArray );
    assertEquals( pattern, null );

  }

  @Test
  public void testDecodeByteBuffer() {
    // TODO: implement
    fail("yet unimplemented");
  }
}

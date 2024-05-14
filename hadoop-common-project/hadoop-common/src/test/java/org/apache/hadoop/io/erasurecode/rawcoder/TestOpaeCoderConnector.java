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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Test preliminary OpaeRSCoding feature.
 */
public class TestOpaeCoderConnector {

  @Test
  public void testNewOpaeCoderConnector() {
    OpaeCoderConnector localOpaeCoderConnector = null;
    try {
      localOpaeCoderConnector = new OpaeCoderConnector();
    }
    catch ( NamingException e ) {
      e.printStackTrace();
      fail("Encountered unexpected NamingException");
    }
    catch ( JMSException e ) {
      e.printStackTrace();
      fail("Encountered unexpected JMSException");
    }
      
    // Assert on class
    assertNotNull( localOpaeCoderConnector );
  }
}

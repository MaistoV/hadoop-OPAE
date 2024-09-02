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

import org.apache.hadoop.io.erasurecode.rawcoder.OpaeCoderConnector;
import org.apache.hadoop.conf.Configuration;

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
public class TestOpaeCoderConnector {
  // Common object
  OpaeCoderConnector localOpaeCoderConnector;

  // @Before
  // public void beforeTest() {
  //   OpaeCoderConnector localOpaeCoderConnector = null;
  //   try {
  //     localOpaeCoderConnector = new OpaeCoderConnector(6, 3);
  //   }
  //   catch ( NamingException e ) {
  //     e.printStackTrace();
  //     fail("Encountered unexpected NamingException");
  //   }
  //   catch ( JMSException e ) {
  //     e.printStackTrace();
  //     fail("Encountered unexpected JMSException");
  //   }
  // }
  
  @Test
  public void testNewOpaeCoderConnectorRS_3_2() {
    try {
      localOpaeCoderConnector = new OpaeCoderConnector(3, 2);
    }
    catch ( NamingException e ) {
      e.printStackTrace();
      fail("Encountered unexpected NamingException");
    }
    catch ( JMSException e ) {
      e.printStackTrace();
      fail("Encountered unexpected JMSException");
    }
      
    // Assert class
    assertNotNull( localOpaeCoderConnector );
    // Assert getters
    assertEquals( localOpaeCoderConnector.getNumDataUnits()  , 3 );
    assertEquals( localOpaeCoderConnector.getNumParityUnits(), 2 );
  }

  @Test
  public void testNewOpaeCoderConnectorRS_6_3() {
    try {
      localOpaeCoderConnector = new OpaeCoderConnector(6, 3);
    }
    catch ( NamingException e ) {
      e.printStackTrace();
      fail("Encountered unexpected NamingException");
    }
    catch ( JMSException e ) {
      e.printStackTrace();
      fail("Encountered unexpected JMSException");
    }
      
    // Assert class
    assertNotNull( localOpaeCoderConnector );
    // Assert getters
    assertEquals( localOpaeCoderConnector.getNumDataUnits()  , 6 );
    assertEquals( localOpaeCoderConnector.getNumParityUnits(), 3 );
  }

  @Test
  public void testConfiguration() {
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
      
    // Assert configs
    Configuration conf = new Configuration();

    // Timeout
    int expectedReceiveTimeoutMillisec = conf.getInt(localOpaeCoderConnector.receiveTimeoutMillisecConfString, 0);
    assertEquals( localOpaeCoderConnector.getReceiveTimeoutMillisecs(), expectedReceiveTimeoutMillisec );

    // SBDF array
    String [] expectedSbdfArray = conf.getStrings(localOpaeCoderConnector.sbdfArrayConfString);
    assertArrayEquals( localOpaeCoderConnector.getSbdfArray(), expectedSbdfArray );
  }

}

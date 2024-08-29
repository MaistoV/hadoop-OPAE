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

 import org.apache.hadoop.classification.InterfaceAudience;
 import org.apache.hadoop.conf.Configuration;
 import java.io.IOException;
 import java.util.Properties;
 import java.util.Arrays;
 
 /**
  * Load configuration fro VFProxy Client. 
  * Most of this code is borrowed from HdfsConfiguration.java.
  */
 @InterfaceAudience.Private
 public class OpaeVFPClientConfiguration extends Configuration {
   static {
     // adds the default resources
     Configuration.addDefaultResource("vfp-client-default.xml");
     Configuration.addDefaultResource("vfp-client-site.xml");
   }
 
   public OpaeVFPClientConfiguration() {
     super();
   }
 
   public OpaeVFPClientConfiguration(boolean loadDefaults) {
     super(loadDefaults);
   }
 
   public OpaeVFPClientConfiguration(Configuration conf) {
     super(conf);
   }
 
   /**
    * This method is here so that when invoked, OpaeVFPClientConfiguration is class-loaded
    * if it hasn't already been previously loaded.  Upon loading the class, the
    * static initializer block above will be executed to add the deprecated keys
    * and to add the default resources. It is safe for this method to be called
    * multiple times as the static initializer block will only get invoked once.
    *
    * This replaces the previously, dangerous practice of other classes calling
    * Configuration.addDefaultResource("hdfs-default.xml") directly without
    * loading this class first, thereby skipping the key deprecation.
    */
   public static void init() {
   }
 
   public static void main( String [] args ) throws IOException {
    // Create configuration
    init();
    OpaeVFPClientConfiguration conf = new OpaeVFPClientConfiguration();

    // Get hardcoded VFP properties
    // NOTE: there is also Configuration.getValByRegex()
    String [] keys = {
      "dfs.datanode.vfp-client.sbdf-array",
      "dfs.datanode.vfp-client.jms.receiveTimeoutMillisec",
      "dfs.datanode.vfp-client.jms.persistentMode",
      "dfs.datanode.vfp-client.jms.msgPriority",
      "dfs.datanode.vfp-client.jms.msgTimeToLiveMillisec"
    };
    for ( String key : keys ) {
      String value = conf.get(key); // Expands variables
      System.out.println( key + "=" + value );
    }

    // Get all properties
    // Properties props = conf.getProps();
    // Get VFP properties by tag
    // Properties props = conf.getAllPropertiesByTag( "VFP" );
    // // Properties props = conf.getAllPropertiesByTags( Arrays.asList("VFP") );
    // // Print to stdout
    // props.list( System.out ); // Does not expand variables
   }
 }
 
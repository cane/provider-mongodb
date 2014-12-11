/**
 * Copyright 2012 CaneData.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.canedata.provider.mongodb.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.LogManager;

import com.mongodb.*;
import org.canedata.CaneProvider;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.provider.mongodb.MongoProvider;
import org.canedata.provider.mongodb.MongoResourceProvider;
import org.canedata.provider.mongodb.command.Truncate;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-3
 */
public abstract class AbstractAbility {
	protected static MongoClient mongo = null;
	protected static CaneProvider provider = null;
	protected static MongoResourceProvider resProvider = null;
	protected static EntityFactory factory = null;
	
	protected static String host = "localhost";
	protected static int port = 27017;
	
	
	protected void initLogManager(){
		LogManager lm = LogManager.getLogManager();
		try {
			lm.readConfiguration(AbilityProvider.class
					.getResourceAsStream("/logging.properties"));
		} catch (SecurityException e) {
			throw e;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected void initConf(){
		Properties conf = new Properties();
		try {
			conf.load(AbilityProvider.class.getResourceAsStream("/conf.properties"));
			host = conf.getProperty("mongo.host");
			port = Integer.parseInt(conf.getProperty("mongo.port"));
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
	}
	
	protected void initProvider(){
		try {
			mongo = new MongoClient(host, port);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		} catch (MongoException e) {
			throw new RuntimeException(e);
		}
		
		resProvider = new MongoResourceProvider(mongo);
		resProvider.setDefaultDbName("users");
		
		provider = new MongoProvider();
	}
	
	protected void initFactory(){
		factory = provider.getFactory("test", resProvider);
	}
	
	protected void initData(){
		Command truncate = new Truncate();
		
		factory.get("t").call(truncate);
        factory.get("list").call(truncate);
		
		Entity e = factory.get("user");
		e.execute(truncate);
		
		e.put("age", 13).put("4up", null).put("4inc", 1).create("id:test:1");
		e.put("age", 13).put("4up", "").put("4inc", 1).create("id:test:2");
		e.put("age", 13).put("4up", "dd").put("4inc", 1).create("id:test:3");
		e.put("age", 16).put("gender", 0).put("vendor", "").create();
		e.put("name", "cane").put("gender", 0).put("vendor", "cane team").create("id:test:a");
		e.put("age", 18).put("name", "cane provider").put("gender", 1).create();
		e.put("age", 19).put("name", "mongo").put("gender", 2).create();
		e.put("age", 63).put("name", "provider").put("gender", 1).put("vendor", "cane").create();

		BasicDBList sub = new BasicDBList();
		for(int i = 0; i < 10; i ++){
			BasicDBObject row = new BasicDBObject();
			row.put("name", "name" + i);

			sub.add(row);
		}
		e.put("name","multi").put("sub", sub).create("multixxx");

		e.close();
	}
}

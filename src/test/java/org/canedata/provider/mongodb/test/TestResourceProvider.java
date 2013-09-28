/**
 * Copyright 2011 CaneData.org
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.LogManager;

import org.canedata.provider.mongodb.MongoResourceProvider;
import org.canedata.resource.Resource;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-3
 */
public class TestResourceProvider {
	static MongoClient mongo = null;
	static String host = null;
	static int port = 0;
	
	@BeforeClass
	public static void baseInit() {
		LogManager lm = LogManager.getLogManager();
		try {
			lm.readConfiguration(TestResourceProvider.class
					.getResourceAsStream("/logging.properties"));
		} catch (SecurityException e) {
			throw e;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		Properties conf = new Properties();
		try {
			conf.load(AbilityProvider.class.getResourceAsStream("/conf.properties"));
			host = conf.getProperty("mongo.host");
			port = Integer.parseInt(conf.getProperty("mongo.port"));
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		
		try {
			mongo = new MongoClient(host, port);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		} catch (MongoException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void cu() {
		MongoResourceProvider provider = new MongoResourceProvider(mongo);
		provider.setDefaultDbName("users");

		Resource<DB> res = provider.getResource();
		assertNotNull(res);
		assertNotNull(res.getRepositories());
		assertTrue(res.getRepositories().size() > 2);

		DB db = res.take();
		assertNotNull(db);
		res.release(db);

		DB db1 = res.take("users");
		assertNotNull(db1);

		assertEquals(db1.getName(), "users");
		res.release(db1);
	}

	@Test
	public void injection() {
		MongoResourceProvider provider = new MongoResourceProvider();
		provider.setMongo(mongo);
		provider.setDefaultDbName("local");

		Resource<DB> res = provider.getResource();
		assertNotNull(res);
		assertNotNull(res.getRepositories());
		assertTrue(res.getRepositories().size() > 2);

		DB db = res.take();
		assertNotNull(db);
		res.release(db);

		DB db1 = res.take("users");
		assertNotNull(db1);

		assertEquals(db1.getName(), "users");
		res.release(db1);
	}

	@Test
	public void threads() {
		final MongoResourceProvider provider = new MongoResourceProvider();
		provider.setMongo(mongo);
		provider.setDefaultDbName("users");

		final Resource<DB> res = provider.getResource();
		assertNotNull(res);
		assertNotNull(res.getRepositories());
		assertTrue(res.getRepositories().size() > 2);
		
		ThreadGroup tg = new ThreadGroup("test");
		for(int i = 0; i < 300; i ++){
			new Thread(tg, new Runnable(){

				public void run() {
					DB db = res.take();
					db.requestStart();
					DBCollection dbc = db.getCollection("user");
					assertNotNull(db);
					
					dbc.find().count();
					res.release(db);
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			}).start();
			
		}
		
		while(tg.activeCount() > 0){
			Thread[] ts = new Thread[tg.activeCount()];
			tg.enumerate(ts);
			for(Thread t : ts){
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

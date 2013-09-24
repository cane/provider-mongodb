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
package org.canedata.provider.mongodb;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.MongoClient;
import org.canedata.resource.Resource;
import org.canedata.resource.ResourceProvider;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public class MongoResourceProvider implements ResourceProvider {
	private static final String NAME = "Resource provider for MongoDB";
	private static final String VENDOR = "Cane team";
	private static final int VERSION = 1;
	private static final Map<String, Object> EXTRAS = new HashMap<String, Object>();
	private static final Map<String, String[]> authentications = new HashMap<String, String[]>();
	
	private String defaultDbName = null;
	
	private MongoClient mongo = null;
	private MongoResource resource;
	
	/**
	 * for inject.
	 */
	public MongoResourceProvider(){
		
	}
	
	public MongoResourceProvider(MongoClient mongo){
		this.mongo = mongo;
	}
	
	public MongoResourceProvider authenticate(String dbName, String user, String password){
		authentications.put(dbName.toLowerCase(), new String[]{user, password});
		
		return this;
	}
	
	public String getName() {
		return NAME;
	}

	public String getVendor() {
		return VENDOR;
	}

	public int getVersion() {
		return VERSION;
	}

	public Map<String, Object> getExtras() {
		return EXTRAS;
	}

	public Object getExtra(String key) {
		return EXTRAS.get(key);
	}

	public synchronized Resource<DB> getResource() {
		if(null == resource){
			resource = new MongoResource(){

				@Override
                MongoClient getMongo() {
					return MongoResourceProvider.this.getMongo();
				}

				@Override
				MongoResourceProvider getProvider() {
					return MongoResourceProvider.this;
				}
				
			};
		}
		
		return resource;
	}
	
	public Map<String, String[]> getAuthentications(){
		return authentications;
	}
	
	public MongoResourceProvider setDefaultDBName(String dbn){
		defaultDbName = dbn;
		
		return this;
	}
	
	public String getDefaultDBName(){
		return defaultDbName;
	}

	public String getDefaultDbName() {
		return defaultDbName;
	}

	public void setDefaultDbName(String defaultDbName) {
		this.defaultDbName = defaultDbName;
	}

	//for inject
	public MongoClient getMongo() {
		return mongo;
	}

	public void setMongo(MongoClient mongo) {
		this.mongo = mongo;
	}

}

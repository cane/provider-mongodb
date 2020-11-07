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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.canedata.provider.mongodb.codecs.BigIntegerCodec;
import org.canedata.provider.mongodb.codecs.StringsCodec;
import org.canedata.resource.Resource;
import org.canedata.resource.ResourceProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  * update to driver 4.1
 *  * @author Sun Yitao
 *  * @version 1.1.0 2020-08-24
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public class MongoResourceProvider implements ResourceProvider<MongoDatabase> {
	private static final String NAME = "Resource provider for MongoDB";
	private static final String VENDOR = "Cane team";
	private static final int VERSION = 2; // since 0.6 support mongo drive 4.1
	private static final Map<String, Object> EXTRAS = new HashMap<String, Object>();
	private static final Map<String, String[]> authentications = new HashMap<String, String[]>();
	
	private String defaultDbName = null;
	
	private MongoClient mongo = null;
	private MongoResource resource;

	private CodecRegistry codecRegistry = null;
	private List<Codec<?>> codecs = new ArrayList(){{
		//add(new BigIntegerCodec());
		//add(new StringsCodec());
	}};
	
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

	public <T> T getExtra(String key) {
		return (T)EXTRAS.get(key);
	}

	public MongoResourceProvider setExtra(String key, Object val) {
		EXTRAS.put(key, val);
		return this;
	}

	public MongoResourceProvider registerCodecs(Codec<?>...codec) {
		this.codecs.addAll(codecs);

		return this;
	}

	public MongoResourceProvider withCodecRegistry(CodecRegistry r) {
		this.codecRegistry = r;

		return this;
	}

	protected List<Codec<?>> getCodecs() {
		return codecs;
	}

	protected CodecRegistry getCodecRegistry() {
		return codecRegistry;
	}

	public synchronized Resource<MongoDatabase> getResource() {
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

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
package org.canedata.provider.mongodb.test.cache;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.canedata.cache.CacheProvider;
import org.canedata.module.ehcache.EhcacheProvider;
import org.canedata.provider.mongodb.MongoProvider;
import org.canedata.provider.mongodb.MongoResourceProvider;
import org.canedata.provider.mongodb.test.AbstractAbility;
import org.junit.BeforeClass;

import java.net.UnknownHostException;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class CacheAbilityProvider extends AbstractAbility {
    static CacheProvider cp = new EhcacheProvider("sampleCache");

	@BeforeClass
	public static void baseInit() {
		CacheAbilityProvider p = new CacheAbilityProvider();
		p.initLogManager();
		p.initConf();
		p.initProvider();
		p.initFactory();
		p.initData();
	}

	@Override
	protected void initFactory() {
		provider = new MongoProvider();
		factory = provider.getFactory("test@cache", resProvider, cp);
	}
	
	
}

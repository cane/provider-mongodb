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

import org.canedata.CaneProvider;
import org.canedata.bench.Bench;
import org.canedata.bench.BenchProvider;
import org.canedata.bench.Pointcut;
import org.canedata.cache.CacheProvider;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.StringUtils;
import org.canedata.entity.EntityFactory;
import org.canedata.logging.Logger;
import org.canedata.module.Module;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.entity.MongoEntityFactory;
import org.canedata.resource.ResourceProvider;

/**
 * update VERSION to 2, and support mongo driver 4.1
 * @author Sun Yitao
 * @version 1.1.000 2020-08024
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public class MongoProvider implements CaneProvider {
	final static Logger logger = LoggerFactory.getLogger(MongoProvider.class);

	private static String NAME = "Cane provider for MongoDB";
	private static String VENDOR = "Cane team";
	private static int VERSION = 2;
	private Map<String, Object> extras = new HashMap<String, Object>();

	public MongoProvider(){
		extras.put("api-level", 1);
	}
	
	public EntityFactory getFactory(String name,
			ResourceProvider resourceProvider) {
		CacheProvider cp = null;
		return getFactory(name, resourceProvider, cp);
	}

	public EntityFactory getFactory(String name,
			final ResourceProvider resourceProvider, final CacheProvider cache) {
		if (StringUtils.isBlank(name))
			name = "MF:" + this.hashCode();

		final String _name = name;
		final String defaultSchema = ((MongoResourceProvider)resourceProvider).getDefaultDBName();

		return new MongoEntityFactory() {

			public String getName() {
				return _name;
			}

			public CacheProvider getCacheProvider() {
				return cache;
			}

			public CaneProvider getProvider() {
				return MongoProvider.this;
			}

			@Override
			protected ResourceProvider getResourceProvider() {
				return resourceProvider;
			}

            protected String getDefaultSchema(){
                return defaultSchema;
            }
		};

	}

	public <A extends Bench> A getFactory(String name,
			ResourceProvider resourceProvider, BenchProvider benchProvider) {
		return getFactory(name, resourceProvider, null, benchProvider);
	}

	public <A extends Bench> A getFactory(String name,
			ResourceProvider resourceProvider, CacheProvider cache, BenchProvider benchProvider) {
		EntityFactory ef = getFactory(name,
				resourceProvider, cache);

		if(logger.isDebug())
            logger.debug("Apply bench(name:{0}) to EntityFactory(name:{1}).",
				benchProvider.getName(), ef.getName());

		Pointcut pointcut = benchProvider.getPointcut(Pointcut.Phase.FACTORY);

		return pointcut.wrapped(ef);
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
		return extras;
	}

	public Object getExtra(String key) {
		return extras.get(key);
	}

	@Override
	public MongoProvider setExtra(String key, Object val) {
		extras.put(key, val);
		return this;
	}
}

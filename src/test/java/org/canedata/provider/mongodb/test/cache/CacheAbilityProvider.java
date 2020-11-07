/**
 * Copyright 2011 CaneData.org
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.canedata.provider.mongodb.test.cache;

import org.canedata.cache.Cacheable;
import org.canedata.core.cache.StringCacheableWrapped;
import org.canedata.module.ehcache.EhcacheEventListener;
import org.canedata.module.ehcache.EhcacheProvider;
import org.canedata.provider.mongodb.MongoProvider;
import org.canedata.provider.mongodb.test.AbstractAbility;
import org.ehcache.Cache;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.junit.BeforeClass;

import javax.sound.midi.Soundbank;

/**
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class CacheAbilityProvider extends AbstractAbility {
    static EhcacheProvider cp = new EhcacheProvider("sampleCache");

    @BeforeClass
    public static void baseInit() {
        CacheAbilityProvider p = new CacheAbilityProvider();
        p.initLogManager();
        p.initConf();
        p.initProvider();
        p.initFactory();
    }

    @Override
    protected void initFactory() {
        cp.registerCacheEventListener(new EhcacheEventListener<String, Cacheable>() {
            @Override
            public boolean match(String schema) {
                return true;
            }

            @Override
            public void init(String schema, Cache cache) {
                System.out.println("registerCacheEventListener: " + schema);
            }

            @Override
            public EventOrdering getOrdering() {
                return EventOrdering.ORDERED;
            }

            @Override
            public EventFiring getFiring() {
                return EventFiring.SYNCHRONOUS;
            }

            @Override
            public EventType[] getTypes() {
                return new EventType[]{EventType.CREATED, EventType.REMOVED, EventType.EXPIRED, EventType.UPDATED};
            }

            @Override
            public void onEvent(CacheEvent<? extends String, ? extends Cacheable> cacheEvent) {
                System.out.println(cacheEvent.getType()+ ": "+cacheEvent.getKey());
            }
        });
        provider = new MongoProvider();
        factory = provider.getFactory("test@cache", resProvider, cp);
    }


}

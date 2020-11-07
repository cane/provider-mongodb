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
package org.canedata.provider.mongodb.field;

import java.util.Set;

import org.bson.Document;
import org.canedata.core.field.AbstractFields;
import org.canedata.core.util.StringUtils;
import org.canedata.field.Field;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.intent.MongoIntent;


/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-6-3
 */
public class MongoFields extends AbstractFields {
	Document target = null;
	String key = null;

	public MongoFields(){
		
	}

	public MongoFields(String key) {
		this.key = key;
	}

	@Override
	public boolean isWrappedFor(Class<?> iface) {
		return iface.isInstance(target);
	}

	@Override
	public <T> T unwrap(Class<T> iface) {
		return iface.cast(target);
	}

	@Override
	public <T> T get(String field) {
		return (T)target.get(field);
	}

	public <T> T get(String field, Class<T> tClass) {
		return tClass.cast(target.get(field));
	}

	public MongoFields(String key,
					   Document target) {
		this.key = key;

		this.target = target;

		if(target.containsKey("_id") && target.get("_id") != null)
			key = key.concat(get("_id").toString());
		else
			key = key.concat(String.valueOf(target.hashCode()));
	}

	public MongoFields putTarget(Document t){
		target = t;
		
		return this;
	}
	
	public MongoFields clone(){
		Document d = new Document();
		d.putAll(target);
		MongoFields nmf = new MongoFields(key, d);
		nmf.isRestored = isRestored;
		nmf.cacheTime = cacheTime;
		
		return nmf;
	}
	
	public MongoFields project(Set<String> prj) {
		if (!prj.isEmpty())
			target.keySet().retainAll(prj);

		return this;
	}

	public String[] getFieldNames() {
		return target.keySet().toArray(new String[target.keySet().size()]);
	}

    public MongoReadableField getField(final String field) {
//		if (!target.keySet().contains(field))
//			return null;// throw new NoSuchFieldException(entity.getIdentity(),
						// field);

		return new MongoReadableField() {
			Object val = target.get(field);
			String label = field;
			
			public String getLabel() {
				return label;
			}

			public Field label(String label) {
				this.label = label;
				
				return this;
			}

			public String label() {
				return label;
			}

			@Override
			protected Fields getFields() {
				return MongoFields.this;
			}

			public String getName() {
				return field;
			}

			public Object get() {
				return val;
			}

			public String typeName() {
				return val == null ? null : val.getClass().getName();
			}

		};
	}

	public boolean exist(String field) {
		return target.containsKey(field) && target.get(field) != null;
	}

	public boolean contains(String field) {
		return target.containsKey(field);
	}
	


	public void reset() {
		isRestored = false;
		cacheTime = -1;
		target = null;
		key = null;
	}

	public Object getKey() {
		return key;
	}

}

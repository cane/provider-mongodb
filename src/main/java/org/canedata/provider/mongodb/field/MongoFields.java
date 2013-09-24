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

import org.bson.types.ObjectId;
import org.canedata.core.field.AbstractFields;
import org.canedata.core.util.ByteUtil;
import org.canedata.field.Field;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.intent.MongoIntent;

import com.mongodb.BasicDBObject;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-6-3
 */
public class MongoFields extends AbstractFields {
	MongoEntity entity = null;
	MongoIntent intent = null;
	BasicDBObject target = null;
	String key = null;

	public MongoFields(){
		
	}
	
	public MongoFields(MongoEntity entity, MongoIntent intent) {
		this.entity = entity;
		this.intent = intent;
	}
	
	public MongoFields(MongoEntity entity, MongoIntent intent,
			BasicDBObject target) {
		this.entity = entity;
		this.intent = intent;

		this.target = target;
		
		key = entity.getKey().concat("#").concat(get("_id").toString());
	}
	
	public MongoFields(MongoEntity entity, MongoIntent intent,
			String column, Object value) {
		this.entity = entity;
		this.intent = intent;

		this.target = new BasicDBObject();
		this.target.put(column, value);
		
		key = entity.getKey().concat("/").concat(column).concat("#").concat(value == null?"":value.toString());
	}

	public MongoFields putTarget(BasicDBObject t){
		target = t;
		
		return this;
	}
	
	public BasicDBObject getTarget(){
		return target;
	}
	
	public MongoFields put(MongoEntity entity, MongoIntent intent, BasicDBObject target){
		this.entity = entity;
		this.intent = intent;
		
		this.target = target;
		
		key = entity.getKey().concat("#").concat(get("_id").toString());
		
		return this;
	}
	
	public MongoFields clone(){
		MongoFields nmf = new MongoFields(entity, intent, (BasicDBObject)target.clone());
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
		if (!target.keySet().contains(field))
			return null;// throw new NoSuchFieldException(entity.getIdentity(),
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
		return target.containsField(field) && target.get(field) != null;
	}

	public boolean contains(String field) {
		return target.containsField(field);
	}
	
	@Override
	public char getChar(String field) {
		return (char) target.getInt(field);
	}

	@Override
	public int getInt(String field) {
		return target.getInt(field);
	}

	@Override
	public double getDouble(String field) {
		return target.getDouble(field);
	}

	@Override
	public byte getByte(String field) {
		return (byte) getInt(field);
	}

	@Override
	public byte[] getBytes(String field) {
		Object t = target.get(field);
		if (null == t)
			return new byte[0];

		// if(Bytes.getType(t) == 5)
		// return (byte[])t;

		return ByteUtil.getBytes(target.get(field));
	}

	@Override
	public long getLong(String field) {
		return target.getLong(field);
	}

	@Override
	public short getShort(String field) {
		return (short) getInt(field);
	}

	@Override
	public float getFloat(String field) {
		Double d = ((BasicDBObject) target).getDouble(field);
		return d.floatValue();
	}

	@Override
	public String getString(String field) {
		Object t = target.get(field);
		if (t == null)
			return null;

		if (t instanceof ObjectId)
			return ((ObjectId) t).toString();

		return ByteUtil.getString(t);
	}

	public void reset() {
		entity = null;
		intent = null;
		isRestored = false;
		cacheTime = -1;
		target = null;
		key = null;
	}

	public String getKey() {
		return key;
	}

}

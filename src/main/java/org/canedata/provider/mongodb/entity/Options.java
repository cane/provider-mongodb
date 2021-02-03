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
package org.canedata.provider.mongodb.entity;

import org.bson.codecs.configuration.CodecRegistry;

/**
 * Remove option MONGO_OPTION, RESET_MONGO_OPTIONS, FIND_AND_REMOVE and add READ_CONCERN.
 *
 * @author Sun Yitao
 * @version 1.1 2020-09-16
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2012-2-22
 */
public interface Options {
	/**
	 * <p>If retain is true.
	 * When the command(such as: count, list, etc.) completes does not do the reset, 
	 * context information to retain the next operation.
	 * </p>
	 * <h3>Example:</h3>
	 * <code>
	 * 	Entity e = ...
	 * 	e.filter(e.expr().equals("name", "cane").filter(...);
	 * 	long count = e.opt(Options.RETAIN, true).count();
	 * 	{@code List<Fields>} rlt = e.list(page.PAGE * page.PAGE_SIZE, page.PAGE_SIZE);
	 * 	page.pages = count/page.PAGE_SIZE;
	 * 	...
	 * </code>
	 */
	public static final String RETAIN = "retain";
	
	/**
	 * boolean type, default is false.
	 * if true, the updated document is returned, otherwise the old document is returned (or it would be lost forever).
	 * use at findAndUpdate, such as:
	 * <pre>
	 * e.opt(Options.RETURN_NEW, true).findAndUpdate(...)
	 * ...
	 * </pre>
	 *
	 */
	public static final String RETURN_NEW = "returnNew";
	
	/**
	 * boolean type, default is false.
	 * If upsert is true, do upsert (insert if document not present).
	 * use at {@link MongoEntity#update}, {@link MongoEntity#updateRange} and {@link MongoEntity#findOneAndUpdate}, such as:
	 * <pre>
	 * e.opt(Options.UPSERT, true).findAndUpdate(...) 
	 * ...
	 * </pre>
	 * 
	 * If {@link #UPSERT} is true and {@link #RETURN_NEW} is false, may return NULL when the data does not exist.
	 */
	public static final String UPSERT = "upsert";

    /**
     * @see com.mongodb.client.MongoCollection#withReadPreference
     */
    public static final String READ_PREFERENCE = "readPreference";

	/**
	 * @see com.mongodb.client.MongoCollection#withCodecRegistry(CodecRegistry) 
	 */
	public static final String ADD_CODEC = "withCodec";

	/**
	 * @see com.mongodb.client.FindIterable#batchSize(int)
	 */
	public static final String BATCH_SIZE = "batchSize";

	/**
	 * @see com.mongodb.client.MongoCollection#withReadConcern
	 */
	public static final String READ_CONCERN = "readConcern";

    /**
     * @see com.mongodb.client.MongoCollection#withWriteConcern(com.mongodb.WriteConcern)
     */
    public static final String WRITE_CONCERN = "writeConcern";

    /**
     * boolean type, default is getCache() != null. IF the value is false,
     * then the cache is invalid during current operation.
     */
    public static final String CACHEABLE = "cacheable";

	/**
	 * com.mongodb.TransactionOptions
	 */
	public static final String TRANSACTION_OPTIONS = "tx_options";
}

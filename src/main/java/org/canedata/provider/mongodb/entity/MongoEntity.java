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
package org.canedata.provider.mongodb.entity;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.canedata.cache.Cache;
import org.canedata.cache.Cacheable;
import org.canedata.core.field.AbstractWritableField;
import org.canedata.core.intent.Limiter;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.StringUtils;
import org.canedata.entity.Batch;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.Joint;
import org.canedata.exception.AnalyzeBehaviourException;
import org.canedata.exception.DataAccessException;
import org.canedata.expression.Expression;
import org.canedata.expression.ExpressionBuilder;
import org.canedata.field.Field;
import org.canedata.field.Fields;
import org.canedata.field.WritableField;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.provider.mongodb.codecs.BigIntegerCodec;
import org.canedata.provider.mongodb.codecs.StringsCodec;
import org.canedata.provider.mongodb.command.Aggregate;
import org.canedata.provider.mongodb.expr.MongoExpression;
import org.canedata.provider.mongodb.expr.MongoExpressionBuilder;
import org.canedata.provider.mongodb.expr.MongoExpressionFactory;
import org.canedata.provider.mongodb.field.MongoFields;
import org.canedata.provider.mongodb.field.MongoWritableField;
import org.canedata.provider.mongodb.intent.MongoIntent;
import org.canedata.provider.mongodb.intent.MongoStep;
import org.canedata.ta.Transaction;
import org.canedata.ta.TransactionHolder;

import javax.sound.midi.Soundbank;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;

/**
 * update for support mongodb 4
 *
 * @author Sun Yitao
 * @author Sun Yat-ton
 * @version 1.00.000 2011-7-29
 */
public abstract class MongoEntity extends Cacheable.Adapter implements Entity {
    protected static final Logger logger = LoggerFactory
            .getLogger(MongoEntity.class);

    public final static String internalCmds = "\\$(inc|set|unset|push|pushAll|addToSet|pop|pull|pullAll|rename|bit)";

    protected boolean hasClosed = false;
    protected static final String CHARSET = "UTF-8";

    protected abstract MongoIntent getIntent();

    abstract MongoCollection<Document> getCollection();

    abstract MongoResource getResource();

    abstract MongoEntityFactory getFactory();

    abstract Cache getCache();

    public String getKey() {
        return getFactory().getName().concat(":").concat(getSchema())
                .concat(":").concat(getName());
    }

    public MongoEntity put(String key, Object value) {
        logger.debug("Put value {0} to {1}.", value, key);

        getIntent().step(MongoStep.PUT, key, value);

        return this;
    }

    public MongoEntity putAll(Map<String, Object> params) {
        if (null == params || params.isEmpty())
            return this;

        for (Entry<String, Object> e : params.entrySet()) {
            put(e.getKey(), e.getValue());
        }

        return this;
    }

    public WritableField field(final String field) {
        if (StringUtils.isBlank(field))
            throw new IllegalArgumentException("You must specify a field name.");

        return new MongoWritableField() {
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

            public String typeName() {
                return value == null ? null : value.getClass().getName();
            }

            public String getName() {
                return field;
            }

            @Override
            protected AbstractWritableField put(String field, Object value) {
                getIntent().step(MongoStep.PUT, field, value);

                return this;
            }

            @Override
            protected MongoEntity getEntity() {
                return MongoEntity.this;
            }

            @SuppressWarnings("unchecked")
            @Override
            protected MongoIntent getIntent() {
                return MongoEntity.this.getIntent();
            }

        };
    }

    public Fields create() {
        return create((Serializable)null);
    }

    /**
     * This method is only for multi-column unique index, and using Mongo
     * default primary key value.
     */
    public Fields create(Map<String, Object> keys) {
        putAll(keys);

        return create();
    }

    public Fields create(Serializable key) {
        validateState();

        if (logger.isDebug())
            logger.debug(
                    "Creating entity, Database is {0}, Collection is {1}, key is {2}.",
                    getSchema(), getName(), key != null?key:"");

        try(IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if(step.step() == MongoStep.PUT && step.getPurpose().startsWith("$"))
                logger.warn("Create operation don`t use {0} ...", step.getPurpose());

            if(step.step() != MongoStep.PUT && step.step() != MongoStep.OPTION)
                return true;
            return false;
        })){
            Document doc = intents.genericFields();

            if (key != null)
                doc.append("_id", key);

            //process options
            if (!intents.options().isEmpty())
                prepareOptions(intents.options());

            InsertOneResult rlt = prepareCollection(intents.options()).insertOne(doc);

            if (key == null)
                doc.put("_id", rlt.getInsertedId().asObjectId().getValue());

            MongoFields fields = new MongoFields(genKey(doc.get("_id")), new Document(){{putAll(doc);}});// wrapped new document, because intents will be reset.

            if (logger.isDebug())
                logger.debug(
                        "Created entity, Database is {0}, Collection is {1}, key is {2}.",
                        getSchema(), getName(), key);

            return fields;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public Fields createOrUpdate() {
        return createOrUpdate((Serializable)null);
    }

    /**
     *
     * @param key
     * @return Return modified result, and contains number of records affected, according to "_MatchedCount", "_ModifiedCount"
     */
    public Fields createOrUpdate(Serializable key) {

        validateState();

        // generate key
        if (key == null) {
            new ObjectId();
        }

        if (logger.isDebug())
            logger.debug(
                    "Creating or Updating entity, Database is {0}, Collection is {1}, key is {2}.",
                    getSchema(), getName(), key);

        try(IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if(step.step() == MongoStep.PUT && step.getPurpose().startsWith("$"))
                logger.warn("Create operation don`t use {0} ...", step.getPurpose());

            if(step.step() != MongoStep.PUT && step.step() != MongoStep.OPTION)
                return true;
            return false;
        })){
            Document _keys = new Document();

            _keys.append("_id", key);

            if (!intents.options().isEmpty())
                prepareOptions(intents.options());

            Document doc = intents.genericFields();

            UpdateResult rlt = prepareCollection(intents.options()).updateOne(_keys, new Document().append("$set", doc), new UpdateOptions().upsert(true));

            doc.put("_id", key);

            MongoFields fields = new MongoFields(this.getKey(), new Document(){{putAll(doc);}});

            // cache
            if (null != getCache()) {
                String cacheKey = genKey(doc.get("_id"));
                if (logger.isDebug())
                    logger.debug("Invalid fields to cache, cache key is {0} ...",
                            cacheKey);
                getCache().remove(cacheKey);
            }

            if (logger.isDebug())
                logger.debug(
                        "Created or Updated entity, Database is {0}, Collection is {1}, key is {2}.",
                        getSchema(), getName(), key);

            return fields;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public Fields createOrUpdate(Map<String, Object> keys) {
        validateState();

        // generate key
        Document _key = new Document();
        if (keys == null || keys.size() == 0) {
            _key.put("_id", new ObjectId());
        } else {
            _key.putAll(keys);
        }

        if (logger.isDebug())
            logger.debug(
                    "Creating or Updating entity, Database is {0}, Collection is {1}, key is {2}.",
                    getSchema(), getName(), _key.toJson());

        try(IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if(step.step() == MongoStep.PUT && step.getPurpose().startsWith("$")) {
                logger.warn("Create operation don`t use {0} ...", step.getPurpose());
                return true;//ignore
            }

            if(step.step() != MongoStep.PUT && step.step() != MongoStep.OPTION)
                return true;

            return false;
        })){
            if (!intents.options().isEmpty())
                prepareOptions(intents.options());

            Document doc = intents.genericFields();

            UpdateResult rlt = prepareCollection(intents.options()).updateOne(_key, new Document().append("$set", doc), new UpdateOptions().upsert(true));

            doc.putAll(_key);

            String cacheKey = getKey();

            for(String i : _key.keySet()){
                cacheKey = cacheKey.concat("#").concat(i).concat(":").concat(_key.get(i).toString());
            }

            MongoFields fields = new MongoFields(cacheKey, new Document(){{putAll(doc);}});

            // cache
            if (null != getCache()) {
                if (logger.isDebug())
                    logger.debug("Invalid fields to cache, cache key is {0} ...",
                            cacheKey);
                getCache().remove(cacheKey);
            }

            if (logger.isDebug())
                logger.debug(
                        "Created or Updated entity, Database is {0}, Collection is {1}, key is {2}.",
                        getSchema(), getName(), _key.toJson());

            return fields;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public MongoEntity projection(String... projection) {
        getIntent().step(MongoStep.PROJECTION, null, (Object[]) projection);

        return this;
    }

    public MongoEntity projection(Document projections) {
        getIntent().step(MongoStep.PROJECTION, "doc", projections);

        return this;
    }

    public MongoEntity select(String... projection) {
        return projection(projection);
    }

    public Fields restore(Serializable key) {
        if (logger.isDebug())
            logger.debug(
                    "Restoring entity, Database is {0}, collection is {1}, key is {2}",
                    getSchema(), getName(), key);


        if (key == null)
            throw new IllegalArgumentException(
                    "Keys must be contain one element.");

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if(step.step() == MongoStep.PUT) {
                logger.warn("Restore operation don`t use put, will be ignore ...");
                return true;//ignore
            }
            return false;
        })) {
            Document query = new Document();

            String cacheKey = genKey(key);//getKey().concat("#").concat(key.toString());

            query.append("_id", key);

            if (!intents.options().isEmpty())
                prepareOptions(intents.options());

            MongoCollection<Document> _collection = prepareCollection(intents.options());
            // use cache
            if (null != getCache() && intents.options().getBoolean(Options.CACHEABLE, true)) {
                MongoFields cachedFs = null;
                if (getCache().isAlive(cacheKey)) {
                    if (logger.isDebug())
                        logger.debug(
                                "Restoring entity from cache, by cache key is {0} ...",
                                cacheKey);

                    cachedFs = (MongoFields) getCache().restore(cacheKey);
                } else {
                    FindIterable<Document> fi = _collection.find(query);
                    fi.batchSize(1).limit(1);

                    Document _r = fi.first();
                    if (_r == null)
                        return null;

                    cachedFs = new MongoFields(cacheKey, _r);
                    getCache().cache(cachedFs);

                    if (logger.isDebug())
                        logger.debug(
                                "Restored entity and put to cache, cache key is {0}.",
                                cachedFs.getKey().toString());
                }

                return cachedFs.clone().project(intents.projections().keySet());
            } else {// no cache
                Document dbo = _collection.find(query).projection(intents.projections()).first();

                if (null == dbo)
                    return null;

                if (logger.isDebug())
                    logger.debug("Restored entity, key is {0}, target is {1}.",
                            key.toString(), dbo.toJson());

                return new MongoFields(cacheKey, dbo);
            }
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public ExpressionBuilder getExpressionBuilder() {
        return new MongoExpressionBuilder(this);
    }

    public ExpressionBuilder expr() {
        return getExpressionBuilder();
    }

    public ExpressionBuilder filter() {
        return expr();
    }

    public MongoEntity filter(Expression expr) {
        if (null == expr)
            return this;

        getIntent().step(MongoStep.FILTER, "filter", expr);

        return this;
    }

    public MongoEntity order(String... orderingTerm) {
        if (null == orderingTerm)
            return this;

        getIntent().step(MongoStep.ORDER, null, (Object[]) orderingTerm);

        return this;
    }

    public MongoEntity orderDESC(String... orderingTerm) {
        if (null == orderingTerm)
            return this;

        getIntent().step(MongoStep.ORDER, "desc", (Object[]) orderingTerm);

        return this;
    }

    public MongoEntity limit(int count) {
        getIntent().step(MongoStep.LIMIT, "limita", count);

        return this;
    }

    public MongoEntity limit(int offset, int count) {
        getIntent().step(MongoStep.LIMIT, "limitb", offset, count);

        return this;
    }

    /**
     * e.filter(e.expr().equals("_id", id)).exists() Equivalent to e.count(e.expr().equals("_id", id)) == 0
     *
     * e.filter(...).exists("field1", "field2"...)
     *
     * can use key and name of columns. first param is key, others is columns.
     * When you check whether the columns exists, if one of column does not
     * exist, than return false.
     */
    public boolean exists(Serializable... fields) {
        if (logger.isDebug())
            logger.debug(
                    "Existing entity, Database is {0}, Collection is {1}, fields is {0}",
                    getSchema(), getName(), Arrays.toString(fields));

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (step.step() == MongoStep.PUT){
                logger.warn("Exists operation don`t use put, the put action will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            MongoExpressionFactory exprFactory = intents.exprFactory();
            //exprFactory.addExpression(new MongoExpressionBuilder().)

            BasicDBObject query = intents.exprFactory().toQuery();

            for (int i = 0; i < fields.length; i++) {
                query.append(fields[i].toString(),
                        new BasicDBObject().append("$exists", true));
            }

            return prepareCollection(intents.options()).countDocuments(query) > 0;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public Fields first() {
        return getOne(false);
    }

    public Fields last() {
        return getOne(true);
    }

    private Fields getOne(boolean last) {
        if (logger.isDebug())
            logger.debug(
                    "Getting {0} entity, Database is {1}, Collection is {2}.",
                    last ? "last" : "first", getSchema(), getName());

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (step.step() == MongoStep.PUT) {
                logger.warn("Find operation don`t use put, the put action will be ignored ...");
                return true;//ignore
            }

            return false;
        })) {
            boolean useCache = null != getCache() && intents.options().getBoolean(Options.CACHEABLE, true);

            BasicDBObject query = intents.exprFactory().toQuery();

            if(!intents.options().isEmpty())
                prepareOptions(intents.options());

            FindIterable<Document> fi = null;
            MongoCollection<Document> _collection = prepareCollection(intents.options());
            if (useCache) {// cache
                fi = _collection.find(query).projection(new Document().append("_id", 1));
            } else {// no cache
                // projection
                if (intents.projections().isEmpty())
                    fi = _collection.find(query);
                else
                    fi = _collection.find(query).projection(intents.projections());
            }

            // sort
            if (!intents.sorter().isEmpty())
                fi.sort(intents.sorter());

            Document dbo = null;
            if (last) {
                long c = _collection.countDocuments(query);
                fi.skip((int) c - 1).limit(1);
            }
            dbo = fi.first();

            if (null == dbo)
                return null;

            MongoFields ele = null;
            Object key = dbo.get("_id");
            String cacheKey = genKey(key);
            if (useCache) {
                System.out.println("cache key:"+cacheKey+", "+getCache().isAlive(cacheKey));
                if (getCache().isAlive(cacheKey)) {// load from cache
                    MongoFields mf = (MongoFields) getCache().restore(
                            cacheKey);
                    if (null != mf) ele = mf.clone();// pooling
                }

                if (null != ele && !intents.projections().isEmpty())
                    ele.project(intents.projections().keySet());

                if (null == ele) {
                    ele = new MongoFields(cacheKey);
                    loadForMissedCacheHits(key, ele, intents.projections().keySet(), intents.options());
                }
            } else {
                ele = new MongoFields(cacheKey, dbo);
            }

            return ele;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public List<Fields> list() {
        return list(-1, -1);
    }

    public List<Fields> list(int count) {
        return list(0, count);
    }

    public List<Fields> list(int offset, int count) {
        if (logger.isDebug())
            logger.debug(
                    "Listing entities, Database is {0}, Collection is {1}, offset is {2}, count is {3}.",
                    getSchema(), getName(), offset, count);
        List<Fields> rlt = new ArrayList<Fields>();

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (step.step() == MongoStep.PUT) {
                logger.warn("The operation don`t use put, it will be ignored ...");
                return true;//ignore
            }

            return false;
        })) {
            boolean useCache = null != getCache() && intents.options().getBoolean(Options.CACHEABLE, true);

            BasicDBObject query = intents.exprFactory().toQuery();

            if (!intents.options().isEmpty())
                prepareOptions(intents.options());

            MongoCollection<Document> _collection = prepareCollection(intents.options());

            FindIterable<Document> fi = null;
            if (useCache) {// cache
                fi = _collection.find(query).projection(new BasicDBObject().append("_id", 1));
            } else {// no cache
                // projection
                if (intents.projections().isEmpty())
                    fi = _collection.find(query);
                else
                    fi = _collection.find(query).projection(intents.projections());
            }

            // sort
            if (!intents.sorter().isEmpty())
                fi.sort(intents.sorter());

            Limiter limiter = intents.limiter();
            if (offset > 0)
                limiter.offset(offset);

            if (count > 0)
                limiter.count(count);

            if (limiter.isLimit()) {
                fi.skip(limiter.offset());
                fi.limit(limiter.count());
                fi.batchSize(Math.min(limiter.count(), intents.options().getInteger(Options.BATCH_SIZE)));
            }

            if (useCache) {
                Map<Object, MongoFields> missedCacheHits = new HashMap<Object, MongoFields>();

                try(MongoCursor<Document> cursor = fi.iterator()) {
                    while (cursor.hasNext()) {
                        Document dbo = cursor.next();
                        Object key = dbo.get("_id");
                        String cacheKey = genKey(key);

                        MongoFields ele = null;
                        if (getCache().isAlive(cacheKey)) {// load from cache
                            MongoFields mf = (MongoFields) getCache().restore(
                                    cacheKey);
                            if (null != mf) ele = mf.clone();
                        }

                        if (null != ele && !intents.projections().isEmpty())
                            ele.project(intents.projections().keySet());

                        if (null == ele) {
                            ele = new MongoFields(cacheKey);
                            missedCacheHits.put(key, ele);
                        }

                        rlt.add(ele);
                    }

                    // load missed cache hits.
                    if (!missedCacheHits.isEmpty()) {
                        loadForMissedCacheHits(missedCacheHits, intents.projections().keySet(), intents.options());
                        missedCacheHits.clear();
                    }

                    if (logger.isDebug())
                        logger.debug("Listed entities hit cache ...");
                }
            } else {
                try(MongoCursor<Document> cursor = fi.iterator()) {
                    while (cursor.hasNext()) {
                        Document dbo = (Document) cursor.next();

                        rlt.add(new MongoFields(genKey(dbo.get("_id")), dbo));
                    }

                    if (logger.isDebug())
                        logger.debug("Listed entities ...");
                }
            }

            return rlt;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public List<Fields> find(Expression expr) {
        this.filter(expr);

        return list();
    }

    public List<Fields> find(Expression expr, int offset, int count) {
        this.filter(expr);

        return list(offset, count);
    }

    public Fields findOne(Expression expr) {
        this.filter(expr);

        return first();
    }

    public Fields findOneAndDelete(Expression filter) {
        if (logger.isDebug())
            logger.debug(
                    "Finding and remove entity, Database is {0}, Collection is {1} ...",
                    getSchema(), getName());

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.PUT, MongoStep.LIMIT).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            intents.exprFactory().addExpression((MongoExpression)filter);// TODO add generic support!!!
            BasicDBObject query = intents.exprFactory().toQuery();

            if (logger.isDebug())
                logger.debug(
                        "Finding and removing entity, Database is {0}, Collection is {1}, expression is {2}, "
                                + "projections is {3}, options is {4} ...",
                        getSchema(), getName(), query.toJson(), intents.projections().toJson(), intents.options().toJson());

            FindOneAndDeleteOptions uo = new FindOneAndDeleteOptions();
            if(intents.hasProjections())
                uo.projection(intents.projections());
            if(intents.hasSorter())
                uo.sort(intents.sorter());

            Document rlt = prepareCollection(intents.options()).findOneAndDelete(query, uo);
            if (rlt == null || rlt.keySet().isEmpty())
                return null;

            // remove cache
            if (null != getCache()) {
                invalidateCache(query, intents.options());
            }

            return new MongoFields(genKey(rlt.get("_id")), rlt).project(intents.projections().keySet());
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    /**
     * Don't use internal command such as $(inc|set|unset|push|pushAll|addToSet|pop|pull|pullAll|rename|bit) etc.
     *
     * @param filter
     * @return
     */
    public Fields findOneAndReplace(Expression filter) {
        if (logger.isDebug())
            logger.debug(
                    "Finding and replace entity, Database is {0}, Collection is {1} ...",
                    getSchema(), getName());

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.LIMIT, MongoStep.PROJECTION).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            if(step.step() == MongoStep.PUT && step.getPurpose().matches(MongoEntity.internalCmds)){
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            intents.exprFactory().addExpression((MongoExpression)filter);
            BasicDBObject query = intents.exprFactory().toQuery();

            if (logger.isDebug()) {
                logger.debug(
                        "Finding and replacing entity, Database is {0}, Collection is {1}, expression is {2}, "
                                + "Projections is {3}, Update is {4}, Sorter is {5}, Options is {6} ...",
                        getSchema(), getName(), query.toJson(), intents.projections().toJson(),
                        intents.genericFields().toJson(), intents.sorter().toJson(), intents.options().toJson());
            }

            FindOneAndReplaceOptions uo = new FindOneAndReplaceOptions();
            if(intents.hasProjections())
            uo.projection(intents.projections());

            if(intents.hasSorter())
                uo.sort(intents.sorter());

            uo.upsert(intents.options().getBoolean(Options.UPSERT, false));
            uo.returnDocument(intents.options().getBoolean(Options.RETURN_NEW, false) ? ReturnDocument.AFTER : ReturnDocument.BEFORE);

            Document rlt = prepareCollection(intents.options()).findOneAndReplace(query, intents.genericFields(), uo);
            if (rlt == null || rlt.keySet().isEmpty())
                return null;

            // alive cache
            if (null != getCache()) {
                invalidateCache(query, intents.options());
            }

            return new MongoFields(this.getKey(), rlt).project(intents.projections().keySet());
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    /**
     * Finds the first document in the query and updates it.
     *
     * @param filter Expression
     * @return Fields
     * @see com.mongodb.client.MongoCollection#findOneAndUpdate
     */
    @Override
    public Fields findOneAndUpdate(Expression filter) {
        if (logger.isDebug())
            logger.debug(
                    "Finding and updating entity, Database is {0}, Collection is {1} ...",
                    getSchema(), getName());

        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.LIMIT, MongoStep.PROJECTION).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            intents.exprFactory().addExpression((MongoExpression)filter);
            BasicDBObject query = intents.exprFactory().toQuery();

            if (logger.isDebug()) {
                Document fs = new Document(intents.genericFields());
                fs.putAll(intents.operationFields());
                logger.debug(
                        "Finding one and update entity, Database is {0}, Collection is {1}, expression is {2}, "
                                + "Projections is {3}, Update is {4}, Sorter is {5}, Options is {6} ...",
                        getSchema(), getName(), query.toJson(), intents.projections().toJson(),
                        fs.toJson(), intents.sorter().toJson(), intents.options().toJson());
            }

            FindOneAndUpdateOptions uo = new FindOneAndUpdateOptions();
            if(intents.hasProjections())
                uo.projection(intents.projections());

            if(intents.hasSorter())
                uo.sort(intents.sorter());

            uo.upsert(intents.options().getBoolean(Options.UPSERT, false));
            uo.returnDocument(intents.options().getBoolean(Options.RETURN_NEW, false) ? ReturnDocument.AFTER : ReturnDocument.BEFORE);

            Document rlt = prepareCollection(intents.options()).findOneAndUpdate(query, new Document(){{
                putAll(intents.operationFields());
                if(intents.hasGenericFields())//if empty don't add $set
                    append("$set", intents.genericFields());
            }}, uo);
            if (rlt == null || rlt.keySet().isEmpty())
                return null;

            // alive cache
            if (null != getCache()) {
                invalidateCache(query, intents.options());
            }

            return new MongoFields(this.getKey(), rlt).project(intents.projections().keySet());
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    private void loadForMissedCacheHits(Object key, MongoFields fs, Set<String> proj, Document options) {
        Map<Object, MongoFields> missed = new HashMap<>();
        missed.put(key, fs);

        loadForMissedCacheHits(missed, proj, options);
    }

    private void loadForMissedCacheHits(Map<Object, MongoFields> missed,
                                        Set<String> proj, Document options) {
        Set<Object> ids = missed.keySet();

        if (logger.isDebug())
            logger.debug("Loading data for missed cache hits, keys is {0}.",
                    Arrays.toString(ids.toArray()));

        Document query = new Document();
        query.append("_id", new Document().append("$in", Arrays.asList(ids.toArray())));

        FindIterable<Document> fi = prepareCollection(options).find(query).batchSize(ids.size());
        try(MongoCursor<Document> cursor = fi.iterator()){
            while (cursor.hasNext()) {
                Document dbo = cursor.next();

                Object id = dbo.get("_id");

                if (logger.isDebug())
                    logger.debug("Loaded data for missed cache hits, _id is {0}.",
                            id.toString());

                MongoFields mf = missed.get(id).putTarget(dbo);

                getCache().cache(mf.clone());
                if (!proj.isEmpty())
                    mf.project(proj);
            }
        }
    }

    public Number count() {
        return count(null);
    }

    /**
     * @param projection will be ignored in mongodb.
     */
    public Number count(String projection) {
        if (logger.isDebug())
            logger.debug(
                    "Listing entities, Database is {0}, Collection is {1}, Projection is {2}.",
                    getSchema(), getName(), projection);


        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.PUT, MongoStep.LIMIT, MongoStep.PROJECTION).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            validateState();


            MongoCollection<Document> _collection = prepareCollection(intents.options());
            if (!intents.hasQuery())
                return _collection.countDocuments();
            else
                return _collection.countDocuments(intents.exprFactory().toQuery());
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    /**
     * the method don't support sort.
     *
     * @param projection
     * @return
     */
    public List<Fields> distinct(String projection) {
        if (logger.isDebug())
            logger.debug(
                    "Distinguishing entities, Database is {0}, Collection is {1}, column is {2} ...",
                    getSchema(), getName(), projection);
        List<Fields> rlt = new ArrayList<Fields>();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.PUT, MongoStep.COUNT).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            validateState();

            DistinctIterable<String> r = prepareCollection(intents.options()).distinct(projection, intents.exprFactory().toQuery(), String.class);
            r.batchSize(intents.limiter().count());

            try (MongoCursor<String> cursor = r.iterator()) {
                while (cursor.hasNext()) {
                    rlt.add(new MongoFields(genKey(null), new Document().append(projection, cursor.next())));
                }
            }

            return rlt;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public List<Fields> distinct(String projection, Expression exp) {
        filter(exp);

        return distinct(projection);
    }

    /**
     * @param target target
     * @deprecated UnsupportedOperation
     */
    public MongoEntity join(Entity target) {
        throw new UnsupportedOperationException("Unsupported operation <join>.");
    }

    /**
     * @param target target
     * @param type   join type
     * @param on     on expr
     * @deprecated UnsupportedOperation
     */
    public MongoEntity joinOn(Entity target, Joint type, String on) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinOn>.");
    }

    /**
     * @param target target entity
     * @param type   join type
     * @param using  using fields
     * @deprecated UnsupportedOperation
     */
    public MongoEntity joinUsing(Entity target, Joint type, String... using) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinUsing>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity join(String table) {
        throw new UnsupportedOperationException("Unsupported operation <join>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity joinOn(String table, Joint type, String on) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinOn>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity joinUsing(String table, Joint type, String... using) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinUsing>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity group(String... on) {
        throw new UnsupportedOperationException(
                "Unsupported operation <group>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity having(String selection, Object... args) {
        throw new UnsupportedOperationException(
                "Unsupported operation <having>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity union(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <union>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity union(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <union>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity unionAll(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <unionAll>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity unionAll(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinUsing>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity except(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <except>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity except(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <except>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public MongoEntity exceptAll(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <exceptAll>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity exceptAll(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <exceptAll>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity intersect(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <intersect>.");
    }

    public Entity intersect(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <intersect>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity intersectAll(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <intersectAll>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity intersectAll(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <intersectAll>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Number max(String projection) {
        throw new UnsupportedOperationException("Unsupported operation <max>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Number min(String projection) {
        throw new UnsupportedOperationException("Unsupported operation <min>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Number sum(String projection) {
        throw new UnsupportedOperationException("Unsupported operation <sum>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Number avg(String projection) {
        throw new UnsupportedOperationException("Unsupported operation <avg>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public String concat(String delimiter, String... projections) {
        throw new UnsupportedOperationException(
                "Unsupported operation <concat>.");
    }

    public long update() {
        return update((Serializable)null);
    }

    public long update(Serializable key) {
        if (logger.isDebug())
            logger.debug(
                    "Updating entity, Database is {0}, Collection is {1}, specified key is {2}.",
                    getSchema(), getName(), key);

        if (key == null)
            return 0;

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.LIMIT, MongoStep.PROJECTION, MongoStep.DISTINCT, MongoStep.COUNT).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            validateState();

            if(!intents.options().isEmpty())
                prepareOptions(intents.options());

            UpdateResult rlt = prepareCollection(intents.options()).updateOne(new Document().append("_id", key), new Document(){{
                putAll(intents.operationFields());
                if(intents.hasGenericFields())//if empty don't add $set
                    append("$set", intents.genericFields());
            }}, new UpdateOptions().upsert(intents.options().getBoolean(Options.UPSERT, false)));


            // invalidate cache
            if (null != getCache()) {
                getCache().remove(genKey(key));

                if (logger.isDebug())
                    logger.debug("Invalidated cache key is {0}.",
                            key.toString());
            }

            return rlt.getModifiedCount();
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public long updateRange(Expression expr) {
        if (logger.isDebug())
            logger.debug(
                    "Updating entities, Database is {0}, Collection is {1} ...",
                    getSchema(), getName());


        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.LIMIT, MongoStep.PROJECTION).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;//ignore
            }

            return false;
        })) {
            intents.exprFactory().addExpression((MongoExpression)expr);

            BasicDBObject query = intents.exprFactory().toQuery();

            if (logger.isDebug())
                logger.debug(
                        "Updating entities, Database is {0}, Collection is {1}, expression is {2} ...",
                        getSchema(), getName(), query.toJson());

            if(!intents.options().isEmpty())
                prepareOptions(intents.options());

            //position move to here, because query is not invalidate when $pull sub document.
            // invalidate cache
            if (null != getCache()) {
                invalidateCache(query, intents.options());
            }

            UpdateResult rlt = prepareCollection(intents.options())
                    .updateMany(query, new Document(){{
                        putAll(intents.operationFields());
                        if(intents.hasGenericFields())//if empty don't add $set
                            append("$set", intents.genericFields());
                    }}, new UpdateOptions()
                            .upsert(intents.options().getBoolean(Options.UPSERT, false)));

            long effected = rlt.getModifiedCount();

            if (logger.isDebug())
                logger.debug(
                        "Updated entities({0}.{1}#expression is {2}), affected {3} ...",
                        getSchema(), getName(), query.toString(), effected);

            return effected;
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    public long delete() {
        return delete((Serializable)null);
    }

    public long delete(Serializable key) {
        if (key == null) {
            logger.warn("System does not know what data you want to update, "
                    + "you must specify the data row identity.");

            return 0;
        }

        if (logger.isDebug())
            logger.debug(
                    "Deleting entitiy, Database is {0}, Collection is {1}, keys is {2}.",
                    getSchema(), getName(), key);


        validateState();

        final Document options = new Document();
        try {
            getIntent().playback(step -> {
                switch (step.step()) {
                    case MongoStep.OPTION:
                        options.append(step.getPurpose(), step.getScalar());
                        break;
                    default:
                        logger.warn(
                                "Step {0} does not apply to activities delete, this step will be ignored.",
                                step.getPurpose());
                        break;
                }

                return null;
            });

            DeleteResult rlt = prepareCollection(options).deleteOne(new BasicDBObject().append("_id", key));

            // invalidate cache
            if (null != getCache()) {
                getCache().remove(genKey(key));

                if (logger.isDebug())
                    logger.debug("Invalidated cache key is {0}.",
                            key.toString());
            }

            return rlt.getDeletedCount();
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        } finally {
            if(!options.getBoolean(Options.RETAIN, false))
                getIntent().reset();
        }
    }

    public long deleteRange(Expression filter) {
        if (logger.isDebug())
            logger.debug(
                    "Deleting entities, Database is {0}, Collection is {1} ...",
                    getSchema(), getName());


        validateState();

        try (IntentParser.ParserResult intents = IntentParser.parse(this, (step) -> {
            if (Arrays.asList(MongoStep.PUT, MongoStep.LIMIT, MongoStep.PROJECTION).indexOf(step.step()) != -1) {
                logger.warn("The operation don`t use {0}, it will be ignored ...", step.getPurpose());
                return true;
            }

            return false;
        })) {
            intents.exprFactory().addExpression((MongoExpression)filter);
            BasicDBObject query = intents.exprFactory().toQuery();

            if (logger.isDebug())
                logger.debug(
                        "Deleting entities, Database is {0}, Collection is {1}, expression is {2} ...",
                        getSchema(), getName(), query.toString());

            // invalidate cache
            if (null != getCache()) {
                invalidateCache(query, intents.options());
            }

            DeleteResult rlt = prepareCollection(intents.options()).deleteMany(query);

            return rlt.getDeletedCount();
        } catch (AnalyzeBehaviourException e) {
            if (logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                        e.getMessage());

            throw new DataAccessException(e);
        }
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Batch batch() {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Transaction openTransaction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity transaction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity transaction(TransactionHolder holder) {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity rollback() {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity commit() {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity end(boolean expr) {
        throw new UnsupportedOperationException();
    }

    public <D> D execute(Command cmd, Object... args) {
        if (logger.isDebug())
            logger.debug("Executing command {0}, args is {2} ...",
                    cmd.describe(), Arrays.toString(args));

        return cmd.execute(getFactory(), getResource(), this, args);
    }

    /**
     * @see #execute(Command, Object...)
     */
    public <D> D call(Command cmd, Object... args) {
        return execute(cmd, args);
    }

    /**
     * @see org.canedata.provider.mongodb.entity.Options
     * @see org.canedata.entity.Entity#opt(java.lang.String,
     * java.lang.Object[])
     */
    public MongoEntity opt(String key, Object... values) {
        getIntent().step(MongoStep.OPTION, key, values);

        return this;
    }

    public Entity relate(String name) {
        return getFactory().get(getResource(), name);
    }

    public Entity relate(String schema, String name) {
        return getFactory().get(getResource(), schema, name);
    }

    public Entity revive() {
        hasClosed = false;

        getIntent().reset();

        getCollection();

        return this;
    }

    public boolean hasClosed() {
        return hasClosed;
    }

    protected void validateState() {
        if (hasClosed())
            throw new IllegalStateException(
                    "Entity have been closed, can use the revive method to reactivate it.");
    }

    /**
     * Wrapped to Collection aggregate.
     * @see com.mongodb.client.MongoCollection#aggregate(List)
     * @param pipeline
     * @return
     */
    public List<Fields> aggregate(List<? extends Bson> pipeline) {
        return execute(new Aggregate(), pipeline, -1);
    }

    public List<Fields> aggregate(List<? extends Bson> pipeline, int limit) {
        return execute(new Aggregate(), pipeline, limit);
    }

    private void invalidateCache(BasicDBObject query, Document options) {
        MongoCollection<Document> _collection = prepareCollection(options);
        long hit_count = _collection.countDocuments(query);

        FindIterable<Document> rlt = _collection.find(query);
        rlt.projection(new BasicDBObject().append("_id", 1)).batchSize(BigInteger.valueOf(hit_count).intValue());
        try (MongoCursor<Document> cursor = rlt.iterator()) {
            logger.debug("Invalidate cache({0}), for {1} ...", query.toString(), hit_count);
            while(cursor.hasNext()) {
                String key = cursor.next().get("_id").toString();
                String cacheKey = getKey().concat("#").concat(key);
                getCache().remove(cacheKey);

                if (logger.isDebug())
                    logger.debug("Invalidated cache key is {0}.", cacheKey);
            }
        }
    }

    private MongoCollection<Document> prepareCollection(Document options) {
        MongoCollection<Document> _col = getCollection();

        if (options.containsKey(Options.READ_CONCERN)) {
            ReadConcern readConcern = (ReadConcern) options.get(Options.READ_CONCERN);
            _col = _col.withReadConcern(readConcern);
        }

        if (options.containsKey(Options.WRITE_CONCERN)) {
            WriteConcern writeConcern = (WriteConcern) options.get(Options.WRITE_CONCERN);
            _col = _col.withWriteConcern(writeConcern);
        }

        if (options.containsKey(Options.READ_PREFERENCE)) {
            ReadPreference readPreference = (ReadPreference) options.get(Options.READ_PREFERENCE);
            _col = _col.withReadPreference(readPreference);
        }

        Codec [] _cs = null;
        if (options.containsKey(Options.ADD_CODEC)) {
            _cs = (Codec[]) options.get(Options.ADD_CODEC);
        } else {
            _cs = new Codec[] { new BigIntegerCodec(), new StringsCodec()};
        }

        CodecRegistry _codec = CodecRegistries.fromRegistries(_col.getCodecRegistry(), CodecRegistries.fromCodecs(_cs));
        _col = _col.withCodecRegistry(_codec);

        return _col;
    }

    private void prepareOptions(Document options) {
        for (String o : options.keySet()) {
            /* for 2.x driver
            if(Options.READ_PREFERENCE.equals(o)){
                if(!(options.get(o) instanceof ReadPreference))
                    throw new MalformedParameterizedTypeException();

                getCollection().withReadPreference ((ReadPreference)options.get(o));
               break;
            }

            if(Options.READ_CONCERN.equals(o)){
                if(!(options.get(o) instanceof ReadConcern))
                    throw new MalformedParameterizedTypeException();

                getCollection().withReadConcern((ReadConcern)options.get(o));
                break;
            }

            if(Options.WRITE_CONCERN.equals(o)){
                if(!(options.get(o) instanceof WriteConcern))
                    throw new MalformedParameterizedTypeException();

                getCollection().withWriteConcern((WriteConcern)options.get(o));
                break;
            }*/
        }
    }

    private String genKey(Object self) {
        if(getKey() == null) return UUID.randomUUID().toString();

        StringBuffer k = new StringBuffer();
        k.append(getKey()).append("#");
        if(self == null)
            k.append(StringUtils.random(6));
        else
            k.append(self.toString());

        return k.toString();
    }

}

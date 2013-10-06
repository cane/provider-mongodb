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

import java.io.Serializable;
import java.lang.reflect.MalformedParameterizedTypeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.canedata.cache.Cache;
import org.canedata.cache.Cacheable;
import org.canedata.core.field.AbstractWritableField;
import org.canedata.core.intent.Step;
import org.canedata.core.intent.Tracer;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.Limiter;
import org.canedata.core.util.StringUtils;
import org.canedata.entity.Batch;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.Joint;
import org.canedata.exception.AnalyzeBehaviourException;
import org.canedata.exception.DataAccessException;
import org.canedata.exception.EntityNotFoundException;
import org.canedata.expression.Expression;
import org.canedata.expression.ExpressionBuilder;
import org.canedata.field.Field;
import org.canedata.field.Fields;
import org.canedata.field.WritableField;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.provider.mongodb.expr.MongoExpression;
import org.canedata.provider.mongodb.expr.MongoExpressionBuilder;
import org.canedata.provider.mongodb.expr.MongoExpressionFactory;
import org.canedata.provider.mongodb.field.MongoFields;
import org.canedata.provider.mongodb.field.MongoWritableField;
import org.canedata.provider.mongodb.intent.MongoIntent;
import org.canedata.provider.mongodb.intent.MongoStep;
import org.canedata.ta.Transaction;
import org.canedata.ta.TransactionHolder;

/**
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

    abstract DBCollection getCollection();

    abstract MongoResource getResource();

    abstract MongoEntityFactory getFactory();

    abstract Cache getCache();

    public String getKey() {
        return getFactory().getName().concat(":").concat(getSchema())
                .concat(":").concat(getName());
    }

    public Entity put(String key, Object value) {
        logger.debug("Put value {0} to {1}.", value, key);

        getIntent().step(MongoStep.PUT, key, value);

        return this;
    }

    public Entity putAll(Map<String, Object> params) {
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

    /**
     * This method is only for multi-column unique index, and using Mongo
     * default primary key value.
     */
    public Fields create(Map<String, Object> keys) {
        putAll(keys);

        return create();
    }

    public Fields create(Serializable... keys) {
        try {
            validateState();

            // generate key
            Object key = null;
            if (keys != null && keys.length > 0) {
                key = keys[0];
            }

            if (logger.isDebug())
                logger.debug(
                        "Creating entity, Database is {0}, Collection is {1}, key is {2}.",
                        getSchema(), getName(), key);

            final BasicDBObject doc = new BasicDBObject();
            final BasicDBObject options = new BasicDBObject();
            getIntent().playback(new Tracer() {

                public Tracer trace(Step step) throws AnalyzeBehaviourException {
                    switch (step.step()) {
                        case MongoStep.PUT:
                            if (logger.isDebug())
                                logger.debug(
                                        "Analyzing behivor, step is {0}, purpose is {1}, scalar is {2}.",
                                        step.step(), step.getPurpose(),
                                        Arrays.toString(step.getScalar()));

                            doc.put(step.getPurpose(),
                                    step.getScalar() == null ? null : step
                                            .getScalar()[0]);
                            break;
                        case MongoStep.OPTION:
                            options.append(step.getPurpose(), step.getScalar()[0]);
                            break;
                        default:
                            logger.warn(
                                    "Step {0} does not apply to activities create, this step will be ignored.",
                                    step.step());
                    }
                    return this;
                }

            });

            if (key != null)
                doc.put("_id", key);

            //process options
            if(!options.isEmpty())
                prepareOptions(options);

            WriteResult rlt = getCollection().insert(doc,
                    getCollection().getWriteConcern());
            if (!StringUtils.isBlank(rlt.getError()))
                throw new DataAccessException(rlt.getError());

            MongoFields fields = new MongoFields(MongoEntity.this,
                    MongoEntity.this.getIntent(), doc);

            // cache
            if (null != getCache()) {
                if(logger.isDebug())
                    logger.debug("Puting fields to cache, cache key is {0} ...",
                        fields.getKey());
                getCache().cache(fields);
            }

            if(logger.isDebug())
                logger.debug(
                    "Created entity, Database is {0}, Collection is {1}, key is {2}.",
                    getSchema(), getName(), key);

            return fields;
        } catch (AnalyzeBehaviourException e) {
            if(logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                    e.getMessage());

            throw new DataAccessException(e);
        } finally {
            getIntent().reset();
        }
    }

    public Fields createOrUpdate(Serializable... keys) {
        try {
            validateState();

            // generate key
            Object key = null;
            if (keys != null && keys.length > 0) {
                key = keys[0];
            }

            if (logger.isDebug())
                logger.debug(
                        "Creating or Updating entity, Database is {0}, Collection is {1}, key is {2}.",
                        getSchema(), getName(), key);

            final BasicDBObject doc = new BasicDBObject();
            final BasicDBObject options = new BasicDBObject();
            getIntent().playback(new Tracer() {

                public Tracer trace(Step step) throws AnalyzeBehaviourException {
                    switch (step.step()) {
                        case MongoStep.PUT:
                            if (logger.isDebug())
                                logger.debug(
                                        "Analyzing behivor, step is {0}, purpose is {1}, scalar is {2}.",
                                        step.step(), step.getPurpose(),
                                        Arrays.toString(step.getScalar()));

                            doc.put(step.getPurpose(),
                                    step.getScalar() == null ? null : step
                                            .getScalar()[0]);
                            break;
                        case MongoStep.OPTION:
                            options.append(step.getPurpose(), step.getScalar()[0]);
                            break;
                        default:
                            logger.warn(
                                    "Step {0} does not apply to activities create, this step will be ignored.",
                                    step.step());
                    }
                    return this;
                }

            });

            if (key != null)
                doc.put("_id", key);

            if(!options.isEmpty())
                prepareOptions(options);

            WriteResult rlt = getCollection().save(doc,
                    getCollection().getWriteConcern());
            if (!StringUtils.isBlank(rlt.getError()))
                throw new DataAccessException(rlt.getError());

            MongoFields fields = new MongoFields(MongoEntity.this,
                    MongoEntity.this.getIntent(), doc);

            // cache
            if (null != getCache()) {
                if(logger.isDebug())
                    logger.debug("Puting fields to cache, cache key is {0} ...",
                        fields.getKey());
                getCache().cache(fields);
            }

            if(logger.isDebug())
                logger.debug(
                    "Created or Updated entity, Database is {0}, Collection is {1}, key is {2}.",
                    getSchema(), getName(), key);

            return fields;
        } catch (AnalyzeBehaviourException e) {
            if(logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                    e.getMessage());

            throw new DataAccessException(e);
        } finally {
            getIntent().reset();
        }
    }

    public Fields createOrUpdate(Map<String, Object> keys) {
        putAll(keys);

        return createOrUpdate();
    }

    public Entity projection(String... projection) {
        getIntent().step(MongoStep.PROJECTION, null, (Object[]) projection);

        return this;
    }

    public Entity select(String... projection) {
        return projection(projection);
    }

    public Fields restore(Serializable... keys) {
        if(logger.isDebug())
            logger.debug(
                "Restoring entity, Database is {0}, collection is {1}, key is {2}",
                getSchema(), getName(), Arrays.toString(keys));

        try {
            validateState();

            if (keys == null || keys.length == 0)
                throw new IllegalArgumentException(
                        "Keys must be contain one element.");

            final BasicDBObject projection = new BasicDBObject();
            final BasicDBObject options = new BasicDBObject();
            getIntent().playback(new Tracer() {

                public Tracer trace(Step step) throws AnalyzeBehaviourException {
                    switch (step.step()) {
                        case MongoStep.PROJECTION:
                            for (Object field : step.getScalar()) {
                                String f = (String) field;
                                projection.put(f, 1);
                            }

                            break;
                        case MongoStep.OPTION:
                            options.append(step.getPurpose(), step.getScalar()[0]);
                            break;
                        default:
                                logger.warn(
                                    "Step {0} does not apply to activities restore, this step will be ignored.",
                                    step.step());
                    }

                    return this;
                }

            });

            BasicDBObject bdbo = new BasicDBObject();
            bdbo.put("_id", keys[0]);

            // cache
            if (null != getCache()) {
                String cacheKey = getKey().concat("#").concat(
                        keys[0].toString());

                MongoFields cachedFs = null;
                if (getCache().isAlive(cacheKey)) {
                    if(logger.isDebug())
                        logger.debug(
                            "Restoring entity from cache, by cache key is {0} ...",
                            cacheKey);

                    cachedFs = (MongoFields) getCache().restore(cacheKey);
                } else {
                    if(!options.isEmpty())
                        prepareOptions(options);

                    BasicDBObject dbo = (BasicDBObject) getCollection()
                            .findOne(bdbo);

                    if (null == dbo)
                        return null;

                    cachedFs = new MongoFields(this, getIntent(), dbo);
                    getCache().cache(cachedFs);

                    if(logger.isDebug())
                        logger.debug(
                            "Restored entity and put to cache, cache key is {0}.",
                            cachedFs.getKey().toString());
                }

                return cachedFs.clone().project(projection.keySet());
            } else {// no cache
                if(!options.isEmpty())
                    prepareOptions(options);

                DBObject dbo = getCollection().findOne(bdbo, projection);

                if(logger.isDebug())
                    logger.debug("Restored entity, key is {0}, target is {1}.",
                        keys[0].toString(), dbo);

                if (null == dbo)
                    return null;

                return new MongoFields(this, getIntent(), (BasicDBObject) dbo);
            }
        } catch (NoSuchElementException nsee) {
            throw new EntityNotFoundException(this.getKey(), keys[0].toString());
        } catch (AnalyzeBehaviourException e) {
            if(logger.isDebug())
                logger.debug(e, "Analyzing behaviour failure, cause by: {0}.",
                    e.getMessage());

            throw new RuntimeException(e);
        } finally {
            getIntent().reset();
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

    public Entity filter(Expression expr) {
        if (null == expr)
            return this;

        getIntent().step(MongoStep.FILTER, "filter", expr);

        return this;
    }

    public Entity order(String... orderingTerm) {
        if (null == orderingTerm)
            return this;

        getIntent().step(MongoStep.ORDER, null, (Object[]) orderingTerm);

        return this;
    }

    public Entity orderDESC(String... orderingTerm) {
        if (null == orderingTerm)
            return this;

        getIntent().step(MongoStep.ORDER, "desc", (Object[]) orderingTerm);

        return this;
    }

    public Entity limit(int count) {
        getIntent().step(MongoStep.LIMIT, "limita", count);

        return this;
    }

    public Entity limit(int offset, int count) {
        getIntent().step(MongoStep.LIMIT, "limitb", offset, count);

        return this;
    }

    /**
     * can use key and name of columns. first param is key, others is columns. <br/>
     * When you check whether the columns exists, if one of column does not
     * exist, than return false.
     */
    public boolean exists(Serializable... keys) {
        if (keys == null || keys.length == 0)
            throw new IllegalArgumentException("You must specify the key!");

        if(logger.isDebug())
            logger.debug(
                "Existing entity, Database is {0}, Collection is {1}, key is {0}",
                getSchema(), getName(), keys[0]);

        getIntent().reset();

        // cache
        if (null != getCache()) {
            String cacheKey = getKey().concat("#").concat(keys[0].toString());

            if (getCache().isAlive(cacheKey)) {
                if(logger.isDebug())
                    logger.debug(
                        "Restoring entity from cache, by cache key is {0} ...",
                        cacheKey);

                MongoFields cachedFs = (MongoFields) getCache().restore(
                        cacheKey);

                if (keys.length < 2)
                    return null != cachedFs;
                else {
                    Set<String> ks = cachedFs.getTarget().keySet();
                    return ks.containsAll(Arrays.asList(keys));
                }
            }

        }

        validateState();

        BasicDBObject query = new BasicDBObject();
        query.put("_id", keys[0]);

        for (int i = 1; i < keys.length; i++) {
            query.append((String) keys[i],
                    new BasicDBObject().append("$exists", true));
        }

        return getCollection().count(query) > 0;
    }

    public Fields first() {
        List<Fields> rlt = list(0, 1);

        if (rlt == null || rlt.isEmpty())
            return null;

        return rlt.get(0);
    }

    public Fields last() {
        long c = opt(Options.RETAIN, true).count().longValue();

        logger.debug("Lasting entity, total of {0} entities.", c);

        return list((int) c - 1, 1).get(0);
    }

    public List<Fields> list() {
        return list(-1, -1);
    }

    public List<Fields> list(int count) {
        return list(0, count);
    }

    public List<Fields> list(int offset, int count) {
        if(logger.isDebug())
            logger.debug(
                "Listing entities, Database is {0}, Collection is {1}, offset is {2}, count is {3}.",
                getSchema(), getName(), offset, count);
        List<Fields> rlt = new ArrayList<Fields>();

        BasicDBObject options = new BasicDBObject();
        DBCursor cursor = null;

        try {
            validateState();

            MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();
            BasicDBObject projection = new BasicDBObject();
            Limiter limiter = new Limiter.Default();
            BasicDBObject sorter = new BasicDBObject();

            IntentParser.parse(getIntent(), expFactory, null, projection,
                    limiter, sorter, options);

            if(!options.isEmpty())
                prepareOptions(options);

            if (null != getCache()) {// cache
                cursor = getCollection().find(expFactory.toQuery(),
                        new BasicDBObject().append("_id", 1));
            } else {// no cache
                // projection
                if (projection.isEmpty())
                    cursor = getCollection().find(expFactory.toQuery());
                else
                    cursor = getCollection().find(expFactory.toQuery(),
                            projection);
            }

            // sort
            if (!sorter.isEmpty())
                cursor.sort(sorter);

            if (offset > 0)
                limiter.offset(offset);

            if (count > 0)
                limiter.count(count);

            if (limiter.offset() > 0)
                cursor.skip(limiter.offset());
            if (limiter.count() > 0)
                cursor.limit(limiter.count());

            if (null != getCache()) {
                Map<Object, MongoFields> missedCacheHits = new HashMap<Object, MongoFields>();

                while (cursor.hasNext()) {
                    BasicDBObject dbo = (BasicDBObject) cursor.next();
                    Object key = dbo.get("_id");
                    String cacheKey = getKey().concat("#").concat(
                            key.toString());

                    MongoFields ele = null;
                    if (getCache().isAlive(cacheKey)) {// load from cache
                        MongoFields mf = (MongoFields) getCache().restore(
                                cacheKey);
                        ele = mf.clone();// pooling
                        if (!projection.isEmpty())
                            ele.project(projection.keySet());
                    } else {
                        ele = new MongoFields(this, getIntent());
                        missedCacheHits.put(key, ele);
                    }

                    rlt.add(ele);
                }

                // load missed cache hits.
                if (!missedCacheHits.isEmpty()) {
                    loadForMissedCacheHits(missedCacheHits, projection.keySet());
                    missedCacheHits.clear();
                }

                if(logger.isDebug())
                    logger.debug("Listed entities hit cache ...");
            } else {
                while (cursor.hasNext()) {
                    BasicDBObject dbo = (BasicDBObject) cursor.next();

                    rlt.add(new MongoFields(this, getIntent(), dbo));
                }

                if(logger.isDebug())
                    logger.debug("Listed entities ...");
            }

            return rlt;
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            if (!options.getBoolean(Options.RETAIN))
                getIntent().reset();

            if (cursor != null)
                cursor.close();
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

    /**
     * Finds the first document in the query and updates it.
     * @see com.mongodb.DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean, com.mongodb.DBObject, boolean, boolean)
     * @param expr
     * @return
     */
    public Fields findAndUpdate(Expression expr) {
        if(logger.isDebug())
            logger.debug(
                "Finding and updating entity, Database is {0}, Collection is {1} ...",
                getSchema(), getName());

        BasicDBObject options = new BasicDBObject();
        try {
            validateState();

            final BasicDBObject fields = new BasicDBObject();

            MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();
            BasicDBObject projection = new BasicDBObject();
            Limiter limiter = new Limiter.Default();
            BasicDBObject sorter = new BasicDBObject();

            IntentParser.parse(getIntent(), expFactory, fields, projection,
                    limiter, sorter, options);

            BasicDBObject query = expFactory.parse((MongoExpression) expr);

            if(logger.isDebug())
                logger.debug(
                    "Finding and updating entity, Database is {0}, Collection is {1}, expression is {2}, "
                            + "Projections is {3}, Update is {4}, Sorter is {5}, Options is {6} ...",
                    getSchema(), getName(), query.toString(), projection.toString(),
                        fields.toString(), sorter.toString(), JSON.serialize(options));

            DBObject rlt = getCollection().findAndModify(query, projection,
                    sorter, options.getBoolean(Options.FIND_AND_REMOVE, false), fields,
                    options.getBoolean(Options.RETURN_NEW, false),
                    options.getBoolean(Options.UPSERT, false));

            if (rlt == null || rlt.keySet().isEmpty())
                return null;

            // alive cache
            if (null != getCache()) {
                invalidateCache(query);
            }

            return new MongoFields(this, getIntent(), (BasicDBObject) rlt).project(projection.keySet());
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            if (!options.getBoolean(Options.RETAIN, false))
                getIntent().reset();
        }
    }

    private void loadForMissedCacheHits(Map<Object, MongoFields> missed,
                                        Set<String> proj) {
        Set<Object> ids = missed.keySet();
        if (logger.isDebug())
            logger.debug("Loading data for missed cache hits, _id is {0}.",
                    Arrays.toString(ids.toArray()));

        BasicDBObject query = new BasicDBObject();
        query.append("_id", new BasicDBObject().append("$in", ids.toArray()));

        DBCursor cursor = null;
        try {
            cursor = getCollection().find(query);
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();

                Object id = dbo.get("_id");

                if(logger.isDebug())
                    logger.debug("Loaded data for missed cache hits, _id is {0}.",
                        id.toString());

                MongoFields mf = missed.get(id).putTarget(dbo);
                getCache().cache(mf.clone());
                if (!proj.isEmpty())
                    mf.project(proj);
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
    }

    public Number count() {
        return count(null);
    }

    /**
     * @param projection will be ignored in mongodb.
     */
    public Number count(String projection) {
        if(logger.isDebug())
            logger.debug(
                "Listing entities, Database is {0}, Collection is {1}, offset is {2}, count is {3}.",
                getSchema(), getName(), 0, 1);
        BasicDBObject options = new BasicDBObject();

        try {
            validateState();

            MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();

            IntentParser.parse(getIntent(), expFactory, null, null, null, null,
                    options);

            BasicDBObject query = expFactory.toQuery();
            if (query == null || query.isEmpty())
                return getCollection().count();
            else
                return getCollection().count(query);
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            if (!options.getBoolean(Options.RETAIN))
                getIntent().reset();
        }
    }

    public List<Fields> distinct(String projection) {
        if(logger.isDebug())
            logger.debug(
                "Distincting entities, Database is {0}, Collection is {1}, column is {2} ...",
                getSchema(), getName(), projection);
        List<Fields> rlt = new ArrayList<Fields>();
        BasicDBObject options = new BasicDBObject();

        try {
            validateState();

            MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();

            IntentParser.parse(getIntent(), expFactory, null, null, null, null,
                    options);

            BasicDBObject query = expFactory.toQuery();

            List r = getCollection().distinct(projection, query);
            for (Object o : r) {
                BasicDBObject dbo = new BasicDBObject();
                dbo.put(projection, o);
                rlt.add(new MongoFields(this, getIntent(), projection, o));
            }
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            if (!options.getBoolean(Options.RETAIN))
                getIntent().reset();
        }

        return rlt;
    }

    public List<Fields> distinct(String projection, Expression exp) {
        filter(exp);

        return distinct(projection);
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity join(Entity target) {
        throw new UnsupportedOperationException("Unsupported operation <join>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity joinOn(Entity target, Joint type, String on) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinOn>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity joinUsing(Entity target, Joint type, String... using) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinUsing>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity join(String table) {
        throw new UnsupportedOperationException("Unsupported operation <join>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity joinOn(String table, Joint type, String on) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinOn>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity joinUsing(String table, Joint type, String... using) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinUsing>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity group(String... on) {
        throw new UnsupportedOperationException(
                "Unsupported operation <group>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity having(String selection, Object... args) {
        throw new UnsupportedOperationException(
                "Unsupported operation <having>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity union(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <union>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity union(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <union>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity unionAll(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <unionAll>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity unionAll(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <joinUsing>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity except(Entity target) {
        throw new UnsupportedOperationException(
                "Unsupported operation <except>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity except(Entity target, String alias) {
        throw new UnsupportedOperationException(
                "Unsupported operation <except>.");
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Entity exceptAll(Entity target) {
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

    public int update(Serializable... keys) {
        if(logger.isDebug())
            logger.debug(
                "Updating entitiy, Database is {0}, Collection is {1}, keys is {2}.",
                getSchema(), getName(), Arrays.toString(keys));

        try {
            if (keys == null || keys.length == 0)
                return 0;

            validateState();

            final BasicDBObject fields = new BasicDBObject();
            final BasicDBObject othersFields = new BasicDBObject();
            final BasicDBObject options = new BasicDBObject();

            getIntent().playback(new Tracer() {

                public Tracer trace(Step step) throws AnalyzeBehaviourException {
                    switch (step.step()) {
                        case MongoStep.PUT:
                            if(logger.isDebug())
                                logger.debug(
                                    "Analyzing behivor PUT, step is {0}, purpose is {1}, scalar is {2}.",
                                    step.step(), step.getPurpose(),
                                    Arrays.toString(step.getScalar()));

                            if (StringUtils.isBlank(step.getPurpose()))
                                break;

                            Object val = (step.getScalar() == null || step
                                    .getScalar().length == 0) ? null : step
                                    .getScalar()[0];

                            if (step.getPurpose().matches(internalCmds))
                                othersFields.append(step.getPurpose(), val);
                            else
                                fields.append(step.getPurpose(), val);

                            break;
                        case MongoStep.OPTION:
                            options.append(step.getPurpose(), step.getScalar()[0]);

                            break;
                        default:
                                logger.warn(
                                    "Step {0} does not apply to activities create, this step will be ignored.",
                                    step.step());
                    }

                    return this;
                }

            });

            BasicDBObject fs = new BasicDBObject();
            if (!fields.isEmpty())
                fs.put("$set", fields);

            if (!othersFields.isEmpty())
                fs.putAll(othersFields.toMap());

            if (fs.isEmpty())
                return 0;

            WriteResult wr = getCollection().update(
                    new BasicDBObject().append("_id", keys[0]), fs, options.getBoolean(Options.UPSERT, false),
                    false, getCollection().getWriteConcern());
            if (!StringUtils.isBlank(wr.getError()))
                throw new DataAccessException(wr.getError());

            // invalidate cache
            if (null != getCache()) {
                String cacheKey = getKey().concat("#").concat(
                        keys[0].toString());
                getCache().remove(cacheKey);

                if (logger.isDebug())
                    logger.debug("Invalidated cache key is {0}.",
                            keys[0].toString());
            }

            return wr.getN();
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            getIntent().reset();
        }
    }

    public int updateRange(Expression expr) {
        if(logger.isDebug())
            logger.debug(
                "Updating entities, Database is {0}, Collection is {1} ...",
                getSchema(), getName());

        try {
            validateState();

            final BasicDBObject fields = new BasicDBObject();
            final BasicDBObject othersFields = new BasicDBObject();
            final BasicDBObject options = new BasicDBObject();

            getIntent().playback(new Tracer() {

                public Tracer trace(Step step) throws AnalyzeBehaviourException {
                    switch (step.step()) {
                        case MongoStep.PUT:
                            if (StringUtils.isBlank(step.getPurpose()))
                                break;

                            Object val = (step.getScalar() == null || step
                                    .getScalar().length == 0) ? null : step
                                    .getScalar()[0];

                            if (step.getPurpose().matches(internalCmds))
                                othersFields.append(step.getPurpose(), val);
                            else
                                fields.append(step.getPurpose(), val);

                            break;
                        case MongoStep.OPTION:
                            options.append(step.getPurpose(), step.getScalar()[0]);

                            break;
                        default:
                                logger.warn(
                                    "Step {0} does not apply to activities create, this step will be ignored.",
                                    step.step());
                    }

                    return this;
                }

            });

            final MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();
            BasicDBObject query = expFactory.parse((MongoExpression) expr);

            if(logger.isDebug())
                logger.debug(
                    "Updating entities, Database is {0}, Collection is {1}, expression is {2} ...",
                    getSchema(), getName(), query.toString());

            BasicDBObject fs = new BasicDBObject();
            if (!fields.isEmpty())
                fs.append("$set", fields);

            if (!othersFields.isEmpty())
                fs.putAll(othersFields.toMap());

            if (fs.isEmpty())
                return 0;

            WriteResult wr = getCollection().update(query, fs, options.getBoolean(Options.UPSERT, false), true,
                    getCollection().getWriteConcern());
            if (!StringUtils.isBlank(wr.getError()))
                throw new DataAccessException(wr.getError());

            // invalidate cache
            if (null != getCache()) {
                invalidateCache(query);
            }

            return wr.getN();
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            getIntent().reset();
        }
    }

    public int delete(Serializable... keys) {
        if (keys == null || keys.length == 0) {
            logger.warn("System does not know what data you want to update, "
                    + "you must specify the data row identity.");

            return 0;
        }

        if(logger.isDebug())
            logger.debug(
                "Deleting entitiy, Database is {0}, Collection is {1}, keys is {2}.",
                getSchema(), getName(), Arrays.toString(keys));

        try {
            validateState();

            WriteResult wr = getCollection().remove(
                    new BasicDBObject().append("_id", keys[0]),
                    getCollection().getWriteConcern());
            if (!StringUtils.isBlank(wr.getError()))
                throw new DataAccessException(wr.getError());

            // invalidate cache
            if (null != getCache()) {
                String cacheKey = getKey().concat("#").concat(
                        keys[0].toString());
                getCache().remove(cacheKey);

                if (logger.isDebug())
                    logger.debug("Invalidated cache key is {0}.",
                            keys[0].toString());
            }

            return wr.getN();
        } finally {
            getIntent().reset();
        }
    }

    public int deleteRange(Expression expr) {
        if(logger.isDebug())
            logger.debug(
                "Deleting entities, Database is {0}, Collection is {1} ...",
                getSchema(), getName());

        try {
            validateState();

            final MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();
            BasicDBObject query = expFactory.parse((MongoExpression) expr);

            if(logger.isDebug())
                logger.debug(
                    "Deleting entities, Database is {0}, Collection is {1}, expression is {2} ...",
                    getSchema(), getName(), query.toString());

            // invalidate cache
            if (null != getCache()) {
                invalidateCache(query);
            }

            WriteResult wr = getCollection().remove(query,
                    getCollection().getWriteConcern());
            if (!StringUtils.isBlank(wr.getError()))
                throw new DataAccessException(wr.getError());

            return wr.getN();
        } catch (AnalyzeBehaviourException abe) {
            if(logger.isDebug())
                logger.debug(abe, "Analyzing behaviour failure, cause by: {0}.",
                    abe.getMessage());

            throw new RuntimeException(abe);
        } finally {
            getIntent().reset();
        }
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Batch<Entity> batch() {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated UnsupportedOperation
     */
    public Batch<Entity> batch(String pattern) {
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
     *      java.lang.Object[])
     */
    public Entity opt(String key, Object... values) {
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

    private void invalidateCache(BasicDBObject query) {
        DBCursor cursor = null;

        try {
            cursor = getCollection().find(query,
                    new BasicDBObject().append("_id", 1));

            while (cursor.hasNext()) {
                String key = cursor.next().get("_id").toString();
                String cacheKey = getKey().concat("#").concat(key);
                getCache().remove(cacheKey);

                if (logger.isDebug())
                    logger.debug("Invalidated cache key is {0}.", cacheKey);
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
    }

    private void prepareOptions(BasicDBObject options){
        for(String o : options.keySet()){
            if(Options.MONGO_OPTION.equals(o)){
                getCollection().addOption(options.getInt(o));
                continue;
            }

            if(Options.RESET_MONGO_OPTIONS.equals(o)){
                getCollection().resetOptions();
                continue;
            }

            if(Options.READ_PREFERENCE.equals(o)){
                if(!(options.get(o) instanceof ReadPreference))
                    throw new MalformedParameterizedTypeException();

               getCollection().setReadPreference((ReadPreference)options.get(o));

               break;
            }

            if(Options.WRITE_CONCERN.equals(o)){
                if(!(options.get(o) instanceof WriteConcern))
                    throw new MalformedParameterizedTypeException();

                getCollection().setWriteConcern((WriteConcern)options.get(o));
                break;
            }
        }
    }

}

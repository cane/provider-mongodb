package org.canedata.provider.mongodb.command;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.StringUtils;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.exception.DataAccessException;
import org.canedata.field.Fields;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.field.MongoFields;
import org.canedata.resource.Resource;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-21 14:18
 */
public class Aggregate implements Command {
    static final Logger logger = LoggerFactory.getLogger(Truncate.class);

    private static final String NAME = "aggregate";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String describe() {
        return "Wrapped for Mongo aggregate.";
    }

    @Override
    public <D> D execute(EntityFactory factory, Resource<?> res, Entity target, Object... args) {
        if(args == null || args.length == 0) {
            throw new DataAccessException("Must specify pipeline, you can refer to MongoCollection.aggregate().");
        }

        int limit = args.length == 2?(int)args[1]:-1;

        int batchSize = limit > 0?limit:100;

        MongoResource _res = (MongoResource)res;

        MongoDatabase db = ((MongoResource)res).take();
        MongoCollection collection = db.getCollection(target.getName());

        AggregateIterable<Document> ai =collection.aggregate((List<? extends Bson>)args[0]);
        ai.batchSize(batchSize);
        List<Fields> _rlt = new ArrayList<>();

        try(MongoCursor<Document> cursor = ai.iterator()){
            int _count = 0;
            while (cursor.hasNext()) {
                if(limit > 0 && _count >= limit)
                    break;

                _count ++;

                _rlt.add(new MongoFields(((String)target.getKey()).concat("aggregate:").concat(StringUtils.random(5)), cursor.next()));
            }
        }

        return (D)_rlt;
    }
}

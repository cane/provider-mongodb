package org.canedata.provider.mongodb.ta;

import com.mongodb.client.ClientSession;
import org.canedata.ta.Transaction;
import org.canedata.ta.TransactionException;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-11-09 09:58
 */
public abstract class MongoTransaction implements Transaction {
    public static interface Status {
        public static final int ACTIVE = 0;
        public static final int COMMITTING = 1;
        public static final int COMMITTED = 2;

        public static final int ROLLING_BACK = 3;
        public static final int ROLLED_BACK = 4;

        public static final int CLOSED = 11;
    }

    private int status = Status.ACTIVE;
    private boolean rollbackOnly = false;

    public abstract ClientSession getSession();

    @Override
    public void commit() throws TransactionException, SecurityException, IllegalStateException {
        if (status == Status.CLOSED)
            throw new IllegalStateException("The transaction has closed!");
        if (rollbackOnly)
            throw new IllegalStateException("The transaction is set to auto-rollback!");

        status = Status.COMMITTING;
        getSession().commitTransaction();
        status = Status.COMMITTED;
    }

    @Override
    public void rollback() throws TransactionException {
        if (status != Status.ACTIVE)
            throw new IllegalStateException("The transaction has inactive!");

        status = Status.ROLLING_BACK;
        getSession().abortTransaction();
        status = Status.ROLLED_BACK;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void setRollbackOnly() {
        rollbackOnly = true;
    }

    @Override
    public void close() {
        if (status == Status.ACTIVE || rollbackOnly)
            try{
                rollback();
            } catch (TransactionException e) {
                throw new RuntimeException(e);
            }

        getSession().close();
        status = Status.CLOSED;
    }

    @Override
    public boolean hasClosed() {
        return status == Status.CLOSED;
    }

    @Override
    public boolean isWrappedFor(Class<?> iface) {
        return iface.isInstance(getSession());
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return iface.cast(getSession());
    }
}

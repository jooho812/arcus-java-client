package net.spy.memcached.ops;

/**
 * Created by jooho on 2018. 8. 28..
 */
public interface RangeGetOperation extends KeyedOperation {

    interface Callback extends OperationCallback {
        void gotData(byte[] key);
    }

}

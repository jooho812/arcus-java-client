package net.spy.memcached.collection;

import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.OperationStatus;

import java.util.concurrent.TimeUnit;
import java.util.List;

/**
 * Created by jooho on 2018. 8. 29..
 */
public class RangeGetTest extends BaseIntegrationTest {
  private String key = "RangeGetTest";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  public void testAfterSuccess() throws Exception {
    CollectionFuture<List<Object>> future;
    List<Object> val;
    OperationStatus status;
    String frkey = "key0";
    String tokey = "key3";

    future = mc.asyncRangeGet(frkey, tokey, 5);

    // OperationStatus should be null before operation completion
    // status = future.getOperationStatus();
    // assertNull(status);

    // After operation completion (SUCCESS)
    val = future.get(5000, TimeUnit.MILLISECONDS);
    status = future.getOperationStatus();

    assert(val.size() > 0);
    assertNotNull(status);
    assertTrue(status.isSuccess());
    assertEquals("END", status.getMessage());
  }
}

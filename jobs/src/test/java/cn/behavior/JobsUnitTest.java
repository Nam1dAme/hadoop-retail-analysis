package cn.behavior;

import java.nio.charset.StandardCharsets;
import java.util.*;

public final class JobsUnitTest {
  private static void require(boolean value, String msg) {
    if (!value) throw new AssertionError(msg);
  }

  private static Event parse(String s) throws Exception {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    return Event.parse(b, b.length, "a".repeat(64), 0);
  }

  public static void main(String[] args) throws Exception {
    // 1. Event/Clean date routing: in-window dates vs outside partition
    Event inStart = parse("1,2,3,pv," + Event.START);
    require("2017-11-25".equals(inStart.partitionDate()), "start date partition");
    Event inEnd = parse("1,2,3,buy," + (Event.END - 1));
    require("2017-12-03".equals(inEnd.partitionDate()), "end date partition");
    Event before = parse("1,2,3,cart," + (Event.START - 1));
    require(!before.scoped() && "outside".equals(before.partitionDate()), "before-window routes to outside");
    Event after = parse("1,2,3,fav," + Event.END);
    require(!after.scoped() && "outside".equals(after.partitionDate()), "after-window routes to outside");
    Event bogusPast = parse("1,2,3,pv,-2147483648");
    require(!bogusPast.scoped() && "outside".equals(bogusPast.partitionDate()), "1901 timestamp routes to outside");
    Event bogusFuture = parse("1,2,3,pv,2147483647");
    require(!bogusFuture.scoped() && "outside".equals(bogusFuture.partitionDate()), "2038 timestamp routes to outside");

    // 2. Profile buckets: deterministic, bounded [1, 16384], thread-safe
    int b1 = Profile.bucket("300");
    require(b1 >= 1 && b1 <= 16384, "bucket range");
    require(b1 == Profile.bucket("300"), "bucket deterministic");
    Thread[] threads = new Thread[4];
    int[] results = new int[4];
    for (int i = 0; i < 4; i++) {
      final int idx = i;
      threads[i] = new Thread(() -> results[idx] = Profile.bucket("cat_" + idx + "_test"));
      threads[i].start();
    }
    for (Thread t : threads) t.join();
    for (int i = 0; i < 4; i++) {
      require(results[i] == Profile.bucket("cat_" + i + "_test"), "thread-local bucket matches");
    }

    // 3. Ranking numeric tie-breaking
    Ranking.Row r2 = new Ranking.Row("2", 100);
    Ranking.Row r10 = new Ranking.Row("10", 100);
    PriorityQueue<Ranking.Row> heap = new PriorityQueue<>(Ranking.WORST);
    heap.add(r2);
    heap.add(r10);
    // Min-heap head is worst: "10" should be evicted before "2"
    require("10".equals(heap.poll().id), "numeric tie-breaker evicts 10 before 2");
    require("2".equals(heap.poll().id), "numeric tie-breaker keeps 2");
    require(Ranking.compareIds("99999999999999999999", "100000000000000000000") < 0, "20-digit BigInteger comparison");
    require(Ranking.compareIds("abc", "abd") < 0, "non-digit string fallback");

    System.out.println("PASS: Event/Clean outside routing, Profile thread-local buckets, Ranking numeric tie-breaking");
  }
}

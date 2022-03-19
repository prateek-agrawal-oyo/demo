package com.example.demo.simple_unique_upsertion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.google.common.collect.Lists;
import com.sun.tools.javac.util.Pair;


@RestController
@RequestMapping("/simple_unique_upsertion_test")
public class SimpleUniqueUpsertController {

  @Value("${spring.datasource.url}")
  private String url;
  @Value("${spring.datasource.username}")
  private String user;
  @Value("${spring.datasource.password}")
  private String password;
  @Autowired
  private SimpleUniqueUpsertTestRepo testRepo;

  Connection getC() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }

  public List<Pair<String, String>> init(int limit) throws SQLException {
    Set<Pair<String, String>> rows = new HashSet<>();
    Set<String> firstSet = new HashSet<>();

    String SALT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
    final int countOfInsert = 100000;
    while (rows.size() < countOfInsert) {
      StringBuilder first = new StringBuilder();
      StringBuilder second = new StringBuilder();
      Random rnd = new Random();

      for (int j = 0; j < 100; j++) {
        first.append(SALT_CHARS.charAt((int) (rnd.nextFloat() * SALT_CHARS.length())));
        second.append(SALT_CHARS.charAt((int) (rnd.nextFloat() * SALT_CHARS.length())));
      }
      final Pair<String, String> row = new Pair<>(first.toString(), second.toString());
      if (!firstSet.contains(first.toString()) && !rows.contains(row)) {
        rows.add(row);
        firstSet.add(first.toString());
      }
    }

    try (Statement stmt = getC().createStatement()) {
      String sql = "DROP TABLE if exists simple_unique_upsert_test";
      stmt.execute(sql);
    }

    try (Statement stmt = getC().createStatement()) {
      String sql = "CREATE TABLE simple_unique_upsert_test(\n"
          + "id serial primary key not null, \n"
          + "first varchar(127),\n"
          + "last varchar(127),\n"
          + "UNIQUE (first, last)\n"
          + ");";
      stmt.execute(sql);
    }

    List<Pair<String, String>> allInsert = new ArrayList<>(rows);
    List<Pair<String, String>> postInsert = allInsert.subList(0, limit / 2);
    List<Pair<String, String>> toInsert = allInsert.subList(limit / 2, countOfInsert);
    List<Pair<String, String>> postUpdate = new ArrayList<>();

    Set<Integer> passedIndexes = new HashSet<>();
    while (postUpdate.size() < limit / 2) {
      final int index = (int) (new Random().nextFloat() * toInsert.size());
      if (!passedIndexes.contains(index)) {
        StringBuilder second = new StringBuilder();
        for (int j = 0; j < 100; j++) {
          second.append(SALT_CHARS.charAt((int) (new Random().nextFloat() * SALT_CHARS.length())));
        }
        final Pair<String, String> updatedPair = new Pair<>(toInsert.get(index).fst,
            second.toString());
        if (!rows.contains(updatedPair)) {
          postUpdate.add(updatedPair);
          passedIndexes.add(index);
        }
      }
    }

    System.out.printf(
        "allInsert: %s, postInsert: %s, toInsert: %s, postUpdate: %s%n",
        allInsert.size(), postInsert.size(), toInsert.size(), postUpdate.size());

    for (List<Pair<String, String>> toInsertBatch : Lists.partition(toInsert, 20000)) {
      try (Statement stmt = getC().createStatement()) {
        stmt.execute("insert into simple_unique_upsert_test(first, last) values " +
            toInsertBatch.stream().map(pair -> String.format("('%s', '%s')", pair.fst, pair.snd))
                .collect(Collectors.joining(", "))
        );
      }
    }
    return new ArrayList<Pair<String, String>>() {{
      addAll(postInsert);
      addAll(postUpdate);
    }};
  }

  @GetMapping(value = "/1")
  public long useQuery(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();
//    for (Pair<String, String> pair : list) {
//      try (Statement stmt = getC().createStatement()) {
//        String sql = String.format(
//            "update simple_unique_upsert_test set last='%s' where first='%s'",
//            pair.snd, pair.fst);
//        stmt.execute(sql);
//      }
//    }
    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/2")
  public long useQueryOnSingleConn(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();
//    try (Statement stmt = getC().createStatement()) {
//      for (Pair<String, String> pair : list) {
//        String sql = String.format(
//            "update simple_unique_upsert_test set last='%s' where first='%s'",
//            pair.snd, pair.fst);
//        stmt.execute(sql);
//      }
//    }
    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/3")
  public long useQueryJPA(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();

    final List<SimpleUniqueUpsertTest> collect = new ArrayList<>();
    for (Pair<String, String> pair : list) {
      final List<SimpleUniqueUpsertTest> existing = testRepo.findByFirst(pair.fst);
      if (existing.size() > 1) {
        throw new RuntimeException("No way :(");
      } else if (existing.size() == 1) {
        // update
        collect.add(new SimpleUniqueUpsertTest(existing.get(0).getId(), pair.fst, pair.snd));
      } else {
        //insert
        collect.add(new SimpleUniqueUpsertTest(pair.fst, pair.snd));
      }
    }
    testRepo.saveAll(collect);

    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/4")
  public long useP(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    try (Statement stmt = getC().createStatement()) {
      stmt.execute(getProcedure());
    }
    long startTime = System.currentTimeMillis();
    try (Statement stmt = getC().createStatement()) {
      StringBuilder xml = new StringBuilder();
      for (Pair<String, String> pair : list) {
        xml.append("<r>").append("<f>").append(pair.fst).append("</f>").append("<l>")
            .append(pair.snd).append("</l>").append("</r>");
      }
      String sql = String.format(
          "CALL bulk_simple_unique_upsert_test(bulk_data:='<rows>%s</rows>'); vacuum analyze temp; vacuum analyze simple_unique_upsert_test;", xml);
      stmt.execute(sql);
    }
    return System.currentTimeMillis() - startTime;
  }
//
//  @GetMapping(value = "/5")
//  public long usePJpa(@RequestParam int limit) throws SQLException {
//    final List<Pair<String, String>> list = init(limit);
//    try (Statement stmt = getC().createStatement()) {
//      stmt.execute(getProcedure());
//    }
//    long startTime = System.currentTimeMillis();
//    StringBuilder xml = new StringBuilder();
//    for (Pair<String, String> pair : list) {
//      xml.append("<r>").append("<f>").append(pair.fst).append("</f>").append("<l>")
//          .append(pair.snd).append("</l>").append("</r>");
//    }
//    String sql = String.format("<rows>%s</rows>", xml);
//    testRepo.bulkSimpleUniqueUpsertTest(sql);
//    return System.currentTimeMillis() - startTime;
//  }

  private String getProcedure() {
    return "CREATE OR REPLACE PROCEDURE public.bulk_simple_unique_upsert_test(IN bulk_data xml)\n"
        + " LANGUAGE plpgsql\n"
        + "AS $procedure$\n"
        + "BEGIN\n"
        + "\n"
        + "\tCREATE TEMPORARY TABLE temp(id int NULL, \"first\" VARCHAR(127), \"last\" VARCHAR(127));\n"
        + "\n"
        + "\tINSERT INTO temp\n"
        + "\tSELECT \tnull,\n"
        + "\t\t\tunnest(xpath('//f/text()', myTempTable.myXmlColumn)), \n"
        + "\t\t\tunnest(xpath('//l/text()', myTempTable.myXmlColumn))\n"
        + "\tFROM unnest(xpath('//r', bulk_data)) AS myTempTable(myXmlColumn);\n"
        + "\n"
        + "\tUPDATE temp\n"
        + "\tSET id=simple_unique_upsert_test.id\n"
        + "\tFROM simple_unique_upsert_test\n"
        + "\tWHERE simple_unique_upsert_test.first=temp.first;\n"
        + "\t\n"
        + "\tINSERT INTO simple_unique_upsert_test(first, last)\n"
        + "\tSELECT first, last FROM temp\n"
        + "\tWHERE id is NULL;\n"
        + "\n"
        + "\tUPDATE simple_unique_upsert_test \n"
        + "\tSET last=temp.last\n"
        + "\tFROM \"temp\"\n"
        + "\tWHERE temp.id is not null and simple_unique_upsert_test.first = \"temp\".first;\n"
        + "\n"
        + "COMMIT;\n"
        + "end;$procedure$\n";
  }
}
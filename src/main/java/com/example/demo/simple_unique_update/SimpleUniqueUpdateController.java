package com.example.demo.simple_unique_update;

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
@RequestMapping("/simple_unique_update_test")
public class SimpleUniqueUpdateController {

  @Value("${spring.datasource.url}")
  private String url;
  @Value("${spring.datasource.username}")
  private String user;
  @Value("${spring.datasource.password}")
  private String password;
  @Autowired
  private SimpleUniqueUpdateTestRepo testRepo;

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
      String sql = "DROP TABLE if exists simple_unique_update_test";
      stmt.execute(sql);
    }

    try (Statement stmt = getC().createStatement()) {
      String sql = "CREATE TABLE simple_unique_update_test(\n"
          + "id serial primary key not null, \n"
          + "first varchar(127),\n"
          + "last varchar(127),\n"
          + "UNIQUE (first, last)\n"
          + ");";
      stmt.execute(sql);
    }

    List<Pair<String, String>> toInsert = new ArrayList<>(rows);
    List<Pair<String, String>> list = new ArrayList<>();

    Set<Integer> passedIndexes = new HashSet<>();
    while (list.size() < limit) {
      final int index = (int) (new Random().nextFloat() * toInsert.size());
      if (!passedIndexes.contains(index)) {
        StringBuilder second = new StringBuilder();
        for (int j = 0; j < 100; j++) {
          second.append(SALT_CHARS.charAt((int) (new Random().nextFloat() * SALT_CHARS.length())));
        }
        final Pair<String, String> updatedPair = new Pair<>(toInsert.get(index).fst,
            second.toString());
        if (!rows.contains(updatedPair)) {
          list.add(updatedPair);
          passedIndexes.add(index);
        }
      }
    }

    System.out.printf(
        "totalRows: %s, toInsert: %s, list: %s%n", rows.size(), toInsert.size(), list.size());

    for (List<Pair<String, String>> toInsertBatch : Lists.partition(toInsert, 20000)) {
      try (Statement stmt = getC().createStatement()) {
        stmt.execute("insert into simple_unique_update_test(first, last) values " +
            toInsertBatch.stream().map(pair -> String.format("('%s', '%s')", pair.fst, pair.snd))
                .collect(Collectors.joining(", "))
        );
      }
    }
    return list;
  }

  @GetMapping(value = "/1")
  public long useQuery(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();
    for (Pair<String, String> pair : list) {
      try (Statement stmt = getC().createStatement()) {
        String sql = String.format(
            "update simple_unique_update_test set last='%s' where first='%s'",
            pair.snd, pair.fst);
        stmt.execute(sql);
      }
    }
    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/2")
  public long useQueryOnSingleConn(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();
    try (Statement stmt = getC().createStatement()) {
      for (Pair<String, String> pair : list) {
        String sql = String.format(
            "update simple_unique_update_test set last='%s' where first='%s'",
            pair.snd, pair.fst);
        stmt.execute(sql);
      }
    }
    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/3")
  public long useQueryJPA(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    long startTime = System.currentTimeMillis();

    final List<SimpleUniqueUpdateTest> collect = new ArrayList<>();
    for (Pair<String, String> pair : list) {
      final List<SimpleUniqueUpdateTest> existing = testRepo.findByFirst(pair.fst);
      if (existing.size() != 1) {
        throw new RuntimeException("No way :(");
      }
      collect.add(new SimpleUniqueUpdateTest(existing.get(0).getId(), pair.fst, pair.snd));
    }
    testRepo.saveAll(collect);

    return System.currentTimeMillis() - startTime;
  }

  @GetMapping(value = "/4")
  public long useP(@RequestParam int limit) throws SQLException {
    final List<Pair<String, String>> list = init(limit);
    try (Statement stmt = getC().createStatement()) {
      stmt.execute(
          "CREATE OR REPLACE PROCEDURE public.bulk_simple_unique_update_test(IN bulk_data xml)\n"
              + " LANGUAGE plpgsql\n"
              + "AS $procedure$\n"
              + "BEGIN\n"
              + "\n"
              + "\tDROP TABLE if EXISTS temp;\n"
              + "\tCREATE TEMPORARY TABLE temp(\"first\" VARCHAR(127), \"last\" VARCHAR(127));\n"
              + "\n"
              + "\tINSERT INTO temp\n"
              + "\tSELECT unnest(xpath('//f/text()', myTempTable.myXmlColumn)), \n"
              + "\t\t\tunnest(xpath('//l/text()', myTempTable.myXmlColumn))\n"
              + "\tFROM unnest(xpath('//r', bulk_data)) AS myTempTable(myXmlColumn);\n"
              + "\n"
              + "\n"
              + "\tUPDATE simple_unique_update_test \n"
              + "\tSET last=temp.last\n"
              + "\tFROM \"temp\"\n"
              + "\tWHERE simple_unique_update_test.first = \"temp\".first;\n"
              + "\n"
              + "COMMIT;\n"
              + "end;$procedure$\n");
    }
    long startTime = System.currentTimeMillis();
    try (Statement stmt = getC().createStatement()) {
      StringBuilder xml = new StringBuilder();
      for (Pair<String, String> pair : list) {
        xml.append("<r>").append("<f>").append(pair.fst).append("</f>").append("<l>")
            .append(pair.snd).append("</l>").append("</r>");
      }
      String sql = String.format(
          "CALL bulk_simple_unique_update_test(bulk_data:='<rows>%s</rows>')", xml);
      stmt.execute(sql);
    }
    return System.currentTimeMillis() - startTime;
  }
}
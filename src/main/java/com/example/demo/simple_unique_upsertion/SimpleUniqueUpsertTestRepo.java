package com.example.demo.simple_unique_upsertion;

import java.util.List;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.CrudRepository;

interface SimpleUniqueUpsertTestRepo extends CrudRepository<SimpleUniqueUpsertTest, Integer> {

  List<SimpleUniqueUpsertTest> findByFirst(String first);

  @Procedure("bulk_simple_unique_upsert_test")
  void bulkSimpleUniqueUpsertTest(String bulk_data);
}

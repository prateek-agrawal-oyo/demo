package com.example.demo.simple_unique_update;

import java.util.List;
import org.springframework.data.repository.CrudRepository;

interface SimpleUniqueUpdateTestRepo extends CrudRepository<SimpleUniqueUpdateTest, Integer> {
  List<SimpleUniqueUpdateTest> findByFirst(String first);
}

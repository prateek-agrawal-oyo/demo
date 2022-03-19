package com.example.demo.simple_unique_update;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Entity
@Table(name = "simple_unique_update_test")
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
class SimpleUniqueUpdateTest {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(columnDefinition = "serial")
  private int id;
  private String first;
  private String last;

  public SimpleUniqueUpdateTest(String first, String last) {
    this.first = first;
    this.last = last;
  }

}

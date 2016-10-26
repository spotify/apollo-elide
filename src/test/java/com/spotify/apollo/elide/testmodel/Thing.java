package com.spotify.apollo.elide.testmodel;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Thing {
  @Id
  public int id;

  public String name;
  public String description = "description";

  public Thing(int id, String name) {
    this.id = id;
    this.name = name;
  }
}

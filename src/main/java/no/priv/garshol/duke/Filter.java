/*
 * Copyright (c) 2014 SmartRecruiters Inc. All Rights Reserved.
 */

package no.priv.garshol.duke;

import java.util.Collection;
import java.util.HashSet;

/**
 * Date: 24.03.2014
 * Time: 13:32
 *
 * @Author Kamil Sobol
 */
public class Filter {
  private String prop;
  private String value;

  public Filter() {
  }

  public Filter(String prop, String value) {
    this.prop = prop;
    this.value = value;
  }

  public String getProp() {
    return prop;
  }

  public void setProp(String prop) {
    this.prop = prop;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }


  public static Collection<Record> filter(Collection<Record> candidates, Collection<Filter> filters) {
    if(filters==null || filters.isEmpty()) return candidates;

    Collection<Record> result = new HashSet<Record>();

    for(Record record:candidates){
      boolean matches = true;
      for(Filter filter:filters){
        if(!filter.getValue().equals(record.getValue(filter.getProp()))){
          matches = false;
          break;
        }
      }
      if(matches){
        result.add(record);
      }
    }
    return result;
  }
}

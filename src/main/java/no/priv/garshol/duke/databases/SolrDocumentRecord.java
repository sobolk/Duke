package no.priv.garshol.duke.databases;

import no.priv.garshol.duke.Record;
import org.apache.solr.common.SolrDocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Wraps a Solr Document to provide a representation of it as a Record.
 */
public class SolrDocumentRecord implements Record {

  private SolrDocument doc;
  private Collection<String> props;

  public SolrDocumentRecord(SolrDocument doc) {
    this.doc = doc;
    this.props = doc.getFieldNames();
    this.props.remove("score");
  }

  public Collection<String> getProperties() {
    return props;
  }

  public String getValue(String prop) {
    return doc.getFirstValue(prop).toString();
  }

  public Collection<String> getValues(String prop) {
    Collection<Object> fieldValues = doc.getFieldValues(prop);
    if (fieldValues.size() == 1)
      return Collections.singleton(fieldValues.iterator().next().toString());

    Collection<String> values = new ArrayList(fieldValues.size());
    for (Object val : fieldValues) {
      values.add(val.toString());
    }
    return values;
  }

  public void merge(Record other) {
    throw new UnsupportedOperationException();
  }

  public String toString() {
    return "[SolrDocumentRecord " + doc.toString() + "]";
  }

}
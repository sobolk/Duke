package no.priv.garshol.duke.databases;


import no.priv.garshol.duke.DukeConfigException;
import no.priv.garshol.duke.DukeException;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.Record;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SolrDatabase extends IndexerDatabase {

  private String url;
  private SolrServer solrServer;


  public SolrDatabase() {
    this.max_search_hits = 1000000;
    this.maintracker = new SolrEstimateResultTracker();
  }

  protected void init() {
    if (solrServer == null) {
      solrServer = new HttpSolrServer(url);
    }
    if (this.overwrite) {
      try {
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
      } catch (SolrServerException e) {
        throw new DukeException(e);
      } catch (IOException e) {
        throw new DukeException(e);
      }
    }
    super.init();
    initialized = true;
  }


  @Override
  public boolean isInMemory() {
    return false;
  }

  @Override
  public void index(Record record) {
    if (!initialized) {
      init();
    }

    if (!overwrite)
      delete(record);

    SolrInputDocument doc = new SolrInputDocument();
    for (String propName : record.getProperties()) {
      Property prop = config.getPropertyByName(propName);
      if (prop == null)
        throw new DukeConfigException("Record has property " + propName +
            " for which there is no configuration");

      String v = record.getValue(propName);
      if (v == null || v.equals(""))
        continue;

      doc.addField(propName, v);

    }

    try {
      solrServer.add(doc);
    } catch (SolrServerException e) {
      throw new DukeException(e);
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }

  @Override
  protected Query parseTokens(String fieldName, String value) {
    return super.parseTokens(fieldName, escapeLucene(value));
  }


  private void delete(Record record) {
    // removes previous copy of this record from the index, if it's there
    Property idProp = config.getIdentityProperties().iterator().next();
    Query q = parseTokens(idProp.getName(), record.getValue(idProp.getName()));
    try {
      solrServer.deleteByQuery(q.toString());
    } catch (IOException e) {
      throw new DukeException(e);
    } catch (SolrServerException e) {
      throw new DukeException(e);
    }
  }

  @Override
  public void commit() {
    try {
      solrServer.commit();
    } catch (SolrServerException e) {
      throw new DukeException(e);
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }

  @Override
  public void close() {
    solrServer.shutdown();
  }


  public void setSolrServer(SolrServer solrServer) {
    this.solrServer = solrServer;
  }


  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  class SolrEstimateResultTracker extends EstimateResultTracker<SolrDocument> {

    @Override
    protected List<SolrDocument> executeQuery(Query query, Filter filter, int limit, Collection<no.priv.garshol.duke.Filter> filters) throws Exception {
      SolrQuery solrQuery = new SolrQuery(query.toString());
      solrQuery.addField("*");
      solrQuery.addField("score");
      solrQuery.setRows(limit);

      if (filters != null) {
        for (no.priv.garshol.duke.Filter myFilter : filters) {
          solrQuery.addFilterQuery(myFilter.getProp() + ":" + "\"" + myFilter.getValue() + "\"");
        }
      }

      return solrServer.query(solrQuery).getResults();
    }

    @Override
    protected double score(SolrDocument hit) {
      return Double.valueOf(hit.getFirstValue("score").toString());
    }

    @Override
    protected Record toRecord(SolrDocument hit) throws Exception {
      return new SolrDocumentRecord(hit);
    }
  }


}

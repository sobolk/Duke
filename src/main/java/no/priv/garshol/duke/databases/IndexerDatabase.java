package no.priv.garshol.duke.databases;

import no.priv.garshol.duke.*;
import no.priv.garshol.duke.comparators.GeopositionComparator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class IndexerDatabase implements Database {

  protected Configuration config;
  protected EstimateResultTracker maintracker;

  protected boolean fuzzy_search;
  protected Analyzer analyzer;
  protected int max_search_hits;
  protected float min_relevance;
  protected boolean overwrite;
  protected boolean initialized = false;

  // helper for geostuff
  protected GeoProperty geoprop;

  // Deichman case:
  //  1 = 40 minutes
  //  4 = 48 minutes
  final static int SEARCH_EXPANSION_FACTOR = 1;

  public IndexerDatabase() {
    this.analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);

    this.max_search_hits = 1000000;
    this.fuzzy_search = true; // on by default
  }

  public void setConfiguration(Configuration config) {
    this.config = config;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public void setMaxSearchHits(int max_search_hits) {
    this.max_search_hits = max_search_hits;
  }

  public void setMinRelevance(float min_relevance) {
    this.min_relevance = min_relevance;
  }

  /**
   * Parses the query. Using this instead of a QueryParser in order
   * to avoid thread-safety issues with Lucene's query parser.
   *
   * @param fieldName the name of the field
   * @param value     the value of the field
   * @return the parsed query
   */
  protected Query parseTokens(String fieldName, String value) {
    BooleanQuery searchQuery = new BooleanQuery();
    if (value != null) {
      Analyzer analyzer = new KeywordAnalyzer();

      try {
        TokenStream tokenStream =
            analyzer.tokenStream(fieldName, new StringReader(value));
        tokenStream.reset();
        CharTermAttribute attr =
            tokenStream.getAttribute(CharTermAttribute.class);

        while (tokenStream.incrementToken()) {
          String term = attr.toString();
          Query termQuery = new TermQuery(new Term(fieldName, term));
          searchQuery.add(termQuery, BooleanClause.Occur.SHOULD);
        }
      } catch (IOException e) {
        throw new DukeException("Error parsing input string '" + value + "' " +
            "in field " + fieldName);
      }
    }

    return searchQuery;
  }

  /**
   * Parses Lucene query.
   *
   * @param required Iff true, return only records matching this value.
   */
  protected void parseTokens(BooleanQuery parent, String fieldName,
                             String value, boolean required) {
    value = escapeLucene(value);
    if (value.length() == 0)
      return;

    try {
      TokenStream tokenStream =
          analyzer.tokenStream(fieldName, new StringReader(value));
      tokenStream.reset();
      CharTermAttribute attr =
          tokenStream.getAttribute(CharTermAttribute.class);

      while (tokenStream.incrementToken()) {
        String term = attr.toString();
        Query termQuery;
        if (fuzzy_search && isFuzzy(fieldName))
          termQuery = new FuzzyQuery(new Term(fieldName, term));
        else
          termQuery = new TermQuery(new Term(fieldName, term));
        parent.add(termQuery, required ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD);
      }
    } catch (IOException e) {
      throw new DukeException("Error parsing input string '" + value + "' " +
          "in field " + fieldName);
    }
  }

  protected String escapeLucene(String query) {
    char[] tmp = new char[query.length() * 2];
    int count = 0;
    for (int ix = 0; ix < query.length(); ix++) {
      char ch = query.charAt(ix);
      if (ch == '*' || ch == '?' || ch == '!' || ch == '&' || ch == '(' ||
          ch == ')' || ch == '-' || ch == '+' || ch == ':' || ch == '"' ||
          ch == '[' || ch == ']' || ch == '~' || ch == '{' || ch == '}' ||
          ch == '^' || ch == '|')
        tmp[count++] = '\\'; // these characters must be escaped
      tmp[count++] = ch;
    }

    return new String(tmp, 0, count).trim();
  }


  private boolean isFuzzy(String fieldName) {
    Comparator c = config.getPropertyByName(fieldName).getComparator();
    return c != null && c.isTokenized();
  }

  /**
   * Controls whether to use fuzzy searches for properties that have
   * fuzzy comparators. True by default.
   */
  public void setFuzzySearch(boolean fuzzy_search) {
    this.fuzzy_search = fuzzy_search;
  }

  public Collection<Record> lookup(Property property, String value) {
    Query query = parseTokens(property.getName(), value);
    return maintracker.doQuery(query);
  }

  /**
   * Look up potentially matching records.
   */
  public Collection<Record> findCandidateMatches(Record record) {
    // if we have a geoprop it means that's the only way to search
    if (geoprop != null) {
      String value = record.getValue(geoprop.getName());
      if (value != null) {
        Filter filter = geoprop.geoSearch(value);
        return maintracker.doQuery(new MatchAllDocsQuery(), filter);
      }
    }

    // ok, we didn't do a geosearch, so proceed as normal.
    // first we build the combined query for all lookup properties
    BooleanQuery query = new BooleanQuery();
    for (Property prop : config.getLookupProperties()) {
      Collection<String> values = record.getValues(prop.getName());
      if (values == null)
        continue;
      for (String value : values)
        parseTokens(query, prop.getName(), value,
            prop.getLookupBehaviour() == Property.Lookup.REQUIRED);
    }

    // do the query
    return maintracker.doQuery(query);
  }

  protected void init(){
    initSpatial();
  }


  /**
   * Checks to see if we need the spatial support, and if so creates
   * the necessary context objects.
   */
  private void initSpatial() {
    // FIXME: for now, we only use geosearch if that's the only way to
    // find suitable records, since we don't know how to combine
    // geosearch ranking with normal search ranking.
    if (config.getLookupProperties().size() != 1)
      return;

    Property prop = config.getLookupProperties().iterator().next();
    if (!(prop.getComparator() instanceof GeopositionComparator))
      return;

    geoprop = new GeoProperty(prop);
  }

  @Override
  public Record findRecordById(String id) {
    if (!initialized) {
      init();
    }
    Property idprop = config.getIdentityProperties().iterator().next();
    for (Record r : lookup(idprop, id))
      if (r.getValue(idprop.getName()).equals(id))
        return r;

    return null; // not found
  }


  /**
   * The tracker is used to estimate the size of the query result
   * we should ask Lucene for. This parameter is the single biggest
   * influence on search performance, but setting it too low causes
   * matches to be missed. We therefore try hard to estimate it as
   * correctly as possible.
   * <p/>
   * The tracker uses a ring buffer of recent result sizes to
   * estimate the result size.
   */
  abstract class EstimateResultTracker<T> {
    private int limit;
    /**
     * Ring buffer containing n last search result sizes, except for
     * searches which found nothing.
     */
    private int[] prevsizes;
    private int sizeix; // position in prevsizes

    public EstimateResultTracker() {
      this.limit = 100;
      this.prevsizes = new int[10];
    }

    public Collection<Record> doQuery(Query query) {
      return doQuery(query, null);
    }

    public Collection<Record> doQuery(Query query, Filter filter) {
      List<Record> matches;
      try {
        List<T> hits;

        int thislimit = Math.min(limit, max_search_hits);
        while (true) {
          hits = executeQuery(query,filter,thislimit);
          if (hits.size() < thislimit || thislimit == max_search_hits)
            break;
          thislimit = thislimit * 5;
        }

        matches = new ArrayList(Math.min(hits.size(), max_search_hits));
        for (int ix = 0; ix < hits.size() &&
            score(hits.get(ix)) >= min_relevance; ix++)

          matches.add(toRecord(hits.get(ix)));

        if (hits.size() > 0) {
          synchronized (this) {
            prevsizes[sizeix++] = matches.size();
            if (sizeix == prevsizes.length) {
              sizeix = 0;
              limit = Math.max((int) (average() * SEARCH_EXPANSION_FACTOR), limit);
            }
          }
        }
      } catch (Exception e) {
        throw new DukeException(e);
      }
      return matches;
    }

    private double average() {
      int sum = 0;
      int ix = 0;
      for (; ix < prevsizes.length && prevsizes[ix] != 0; ix++)
        sum += prevsizes[ix];
      return sum / (double) ix;
    }


    protected abstract List<T> executeQuery(Query query, Filter filter, int limit) throws Exception;
    protected abstract double score(T hit);
    protected abstract Record toRecord(T hit) throws Exception;
  }
}

package no.priv.garshol.duke.databases;

import no.priv.garshol.duke.*;
import no.priv.garshol.duke.comparators.GeopositionComparator;
import no.priv.garshol.duke.utils.Utils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.Filter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Represents the Lucene index, and implements record linkage services
 * on top of it.
 */
public class LuceneDatabase extends IndexerDatabase {

  private IndexWriter iwriter;
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;

  private String path;


  public LuceneDatabase() {
    this.maintracker = new LuceneEstimateResultTracker();
  }


  /**
   * Returns the path to the Lucene index directory. If null, it means
   * the Lucene index is kept in-memory.
   */
  public String getPath() {
    return path;
  }

  /**
   * The path to the Lucene index directory. If null or not set, it
   * means the Lucene index is kept in-memory.
   */
  public void setPath(String path) {
    this.path = path;
  }

  /**
   * Returns true iff the Lucene index is held in memory rather than
   * on disk.
   */
  public boolean isInMemory() {
    return (directory instanceof RAMDirectory);
  }

  /**
   * Add the record to the index.
   */
  public void index(Record record) {
    if (directory == null)
      init();

    if (!overwrite && path != null)
      delete(record);

    Document doc = new Document();
    for (String propname : record.getProperties()) {
      Property prop = config.getPropertyByName(propname);
      if (prop == null)
        throw new DukeConfigException("Record has property " + propname +
            " for which there is no configuration");

      if (prop.getComparator() instanceof GeopositionComparator &&
          geoprop != null) {
        // index specially as geocoordinates

        String v = record.getValue(propname);
        if (v == null || v.equals(""))
          continue;

        // this gives us a searchable geoindexed value
        for (IndexableField f : geoprop.createIndexableFields(v))
          doc.add(f);

        // this preserves the coordinates in readable form for display purposes
        doc.add(new Field(propname, v, Field.Store.YES,
            Field.Index.NOT_ANALYZED));
      } else {
        Field.Index ix;
        if (prop.isIdProperty())
          ix = Field.Index.NOT_ANALYZED; // so findRecordById will work
        else // if (prop.isAnalyzedProperty())
          ix = Field.Index.ANALYZED;
        // FIXME: it turns out that with the StandardAnalyzer you can't have a
        // multi-token value that's not analyzed if you want to find it again...
        // else
        //   ix = Field.Index.NOT_ANALYZED;

        for (String v : record.getValues(propname)) {
          if (v.equals(""))
            continue; // FIXME: not sure if this is necessary

          doc.add(new Field(propname, v, Field.Store.YES, ix));
        }
      }
    }

    try {
      iwriter.addDocument(doc);
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }

  private void delete(Record record) {
    // removes previous copy of this record from the index, if it's there
    Property idprop = config.getIdentityProperties().iterator().next();
    Query q = parseTokens(idprop.getName(), record.getValue(idprop.getName()));
    try {
      iwriter.deleteDocuments(q);
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }

  /**
   * Flushes all changes to disk.
   */
  public void commit() {
    if (directory == null)
      return;

    try {
      if (reader != null)
        reader.close();

      // it turns out that IndexWriter.optimize actually slows
      // searches down, because it invalidates the cache. therefore
      // not calling it any more.
      // http://www.searchworkings.org/blog/-/blogs/uwe-says%3A-is-your-reader-atomic
      // iwriter.optimize();

      iwriter.commit();
      openSearchers();
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }


  /**
   * Stores state to disk and closes all open resources.
   */
  public void close() {
    if (directory == null)
      return;

    try {
      iwriter.close();
      directory.close();
      if (reader != null)
        reader.close();
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }

  public String toString() {
    return "LuceneDatabase, max-search-hits: " + max_search_hits +
        ", min-relevance: " + min_relevance + ", fuzzy=" + fuzzy_search +
        "\n  " + directory;
  }

  // ----- INTERNALS

  protected void init() {
    try {
      openIndexes(overwrite);
      openSearchers();
      super.init();
      initialized = true;
    } catch (IOException e) {
      throw new DukeException(e);
    }
  }

  private void openIndexes(boolean overwrite) {
    if (directory == null) {
      try {
        if (path == null)
          directory = new RAMDirectory();
        else {
          //directory = new MMapDirectory(new File(config.getPath()));
          // as per http://wiki.apache.org/lucene-java/ImproveSearchingSpeed
          // we use NIOFSDirectory, provided we're not on Windows
          if (Utils.isWindowsOS())
            directory = FSDirectory.open(new File(path));
          else
            directory = NIOFSDirectory.open(new File(path));
        }

        IndexWriterConfig cfg =
            new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
        cfg.setOpenMode(overwrite ? IndexWriterConfig.OpenMode.CREATE :
            IndexWriterConfig.OpenMode.APPEND);
        iwriter = new IndexWriter(directory, cfg);
        iwriter.commit(); // so that the searcher doesn't fail
      } catch (IndexNotFoundException e) {
        if (!overwrite) {
          // the index was not there, so make a new one
          directory = null; // ensure we really do try again
          openIndexes(true);
        } else
          throw new DukeException(e);
      } catch (IOException e) {
        throw new DukeException(e);
      }
    }
  }

  public void openSearchers() throws IOException {
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }


  class LuceneEstimateResultTracker extends EstimateResultTracker<ScoreDoc> {
    @Override
    protected List<ScoreDoc> executeQuery(Query query, Filter filter, int limit, Collection<no.priv.garshol.duke.Filter> filters) throws Exception {
      if(filters!=null &&!filters.isEmpty()){
        BooleanQuery booleanQuery = new BooleanQuery();
        if(query instanceof BooleanQuery){
          booleanQuery = (BooleanQuery) query;
        } else {
          booleanQuery.add(query, BooleanClause.Occur.MUST);
        }
        for(no.priv.garshol.duke.Filter myFilter:filters){
          parseTokens(booleanQuery,myFilter.getProp(),myFilter.getValue(),true);
        }
        query = booleanQuery;
      }
      return Arrays.asList(searcher.search(query, filter, limit).scoreDocs);
    }

    @Override
    protected double score(ScoreDoc doc) {
      return doc.score;
    }

    @Override
    protected Record toRecord(ScoreDoc hit) throws Exception {
      return new DocumentRecord(hit.doc,
          searcher.doc(hit.doc));
    }
  }
}
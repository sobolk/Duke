package no.priv.garshol.duke.test;

import no.priv.garshol.duke.Configuration;
import no.priv.garshol.duke.Database;
import no.priv.garshol.duke.databases.SolrDatabase;
import no.priv.garshol.duke.utils.SolrConfigGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import java.io.File;

public class SolrDatabaseTest extends PersistentDatabaseTest {

  private SolrServer solrServer;
  private CoreContainer container;


  public Database createDatabase(Configuration config) {
    try {
      File configFile = new File(tmpdir.getRoot(), "solr.xml");
      FileUtils.copyInputStreamToFile(SolrDatabaseTest.class.getResourceAsStream("/solr/solr.xml"), configFile);

      File myIndexConfDir = new File(tmpdir.getRoot(), "my-index/conf");
      myIndexConfDir.mkdirs();

      SolrConfigGenerator configGenerator = new SolrConfigGenerator(this.config);

      FileUtils.writeStringToFile(new File(myIndexConfDir, "schema.xml"), configGenerator.generateSchema());
      FileUtils.writeStringToFile(new File(myIndexConfDir, "solrconfig.xml"), configGenerator.generateSolrConfig());


      container = new CoreContainer(tmpdir.getRoot().getAbsolutePath(), configFile);

      solrServer = new EmbeddedSolrServer(container, "my-index");


      SolrDatabase db = new SolrDatabase();
      db.setOverwrite(false);
      db.setConfiguration(config);
      db.setSolrServer(solrServer);
      return db;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


}
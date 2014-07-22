package no.priv.garshol.duke.test;

import no.priv.garshol.duke.Configuration;
import no.priv.garshol.duke.Database;
import no.priv.garshol.duke.databases.SolrDatabase;
import no.priv.garshol.duke.utils.SolrConfigGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.Config;
import org.apache.solr.core.ConfigSolrXmlOld;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;

import java.io.File;

public class SolrDatabaseTest extends PersistentDatabaseTest {

  private SolrServer solrServer;
  private CoreContainer container;


  public Database createDatabase(Configuration config) {
    try {
      File oberConf = new File(tmpdir.getRoot(), "/conf");
      oberConf.mkdirs();
      File configFile = new File(oberConf, "solr.xml");
      FileUtils.copyInputStreamToFile(SolrDatabaseTest.class.getResourceAsStream("/solr/solr.xml"), configFile);

      File myIndexConfDir = new File(tmpdir.getRoot(), "my-index/conf");
      myIndexConfDir.mkdirs();

      SolrConfigGenerator configGenerator = new SolrConfigGenerator(this.config);

      FileUtils.writeStringToFile(new File(myIndexConfDir, "schema.xml"), configGenerator.generateSchema());
      FileUtils.writeStringToFile(new File(myIndexConfDir, "solrconfig.xml"), configGenerator.generateSolrConfig());


      SolrResourceLoader solrResourceLoader = new SolrResourceLoader(tmpdir.getRoot().getAbsolutePath());
      container = new CoreContainer(solrResourceLoader, new ConfigSolrXmlOld(new Config(solrResourceLoader, "solr.xml"), "solr.xml"));
      container.load();
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
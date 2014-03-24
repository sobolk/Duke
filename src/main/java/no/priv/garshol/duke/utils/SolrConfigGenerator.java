package no.priv.garshol.duke.utils;

import no.priv.garshol.duke.Configuration;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.comparators.GeopositionComparator;

public class SolrConfigGenerator {
  private final Configuration config;


  public SolrConfigGenerator(Configuration config) {
    this.config = config;
  }

  public String generateSchema() {
    StringBuilder schema = new StringBuilder();
    schema.append("<schema>");

    schema.append("<types>");
    schema.append("<fieldType name=\"id\" class=\"solr.StrField\"/>");
    schema.append("<fieldType name=\"geo\" class=\"solr.StrField\"/>");
    schema.append("<fieldType name=\"text\" class=\"solr.TextField\">");
    schema.append("<analyzer>\n" +
        "                <tokenizer class=\"solr.StandardTokenizerFactory\"/>\n" +
        "                <filter class=\"solr.WordDelimiterFilterFactory\" generateWordParts=\"1\" generateNumberParts=\"1\"\n" +
        "                        catenateWords=\"1\" catenateNumbers=\"1\" catenateAll=\"0\" splitOnCaseChange=\"1\"/>\n" +
        "                <filter class=\"solr.LowerCaseFilterFactory\"/>\n" +
        "                <filter class=\"solr.PorterStemFilterFactory\"/>\n" +
        "                <filter class=\"solr.RemoveDuplicatesTokenFilterFactory\"/>\n" +
        "            </analyzer>");
    schema.append("</fieldType>");

    schema.append("</types>");

    schema.append("<fields>");

    for (Property property : config.getProperties()) {
      String type = "text";
      if (property.isIdProperty()) {
        type = "id";
      } else if (property.getComparator() instanceof GeopositionComparator) {
        type = "geo";
      }

      String field = String.format(
          "<field name=\"%s\" type=\"%s\" indexed=\"true\" stored=\"true\"/>",
          property.getName(),
          type
      );
      schema.append(field);
    }

    schema.append("</fields>");
    schema.append("</schema>");
    return schema.toString();
  }

  public String generateSolrConfig() {
    StringBuilder solrConfig = new StringBuilder();
    solrConfig.append("<config>");
    solrConfig.append("<luceneMatchVersion>LUCENE_40</luceneMatchVersion>");

    solrConfig.append("<requestHandler name=\"/update\" class=\"solr.UpdateRequestHandler\"/>");

    solrConfig.append("<requestHandler name=\"search\" class=\"solr.SearchHandler\" default=\"true\">");
    solrConfig.append("<lst name=\"defaults\">");
    solrConfig.append("<str name=\"defType\">lucene</str>");
    solrConfig.append("<str name=\"fl\">*,score</str>");
    solrConfig.append("</lst>");
    solrConfig.append("</requestHandler>");

    solrConfig.append("</config>");

    return solrConfig.toString();
  }


}
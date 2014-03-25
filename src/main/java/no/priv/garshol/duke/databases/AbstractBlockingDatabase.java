
package no.priv.garshol.duke.databases;

import no.priv.garshol.duke.*;

import java.util.*;

import static no.priv.garshol.duke.Filter.filter;

/**
 * An abstract database using blocking to find candidate records. It
 * has different concrete implementations depending on where the
 * actual data is stored.
 * @since 1.2
 */
public abstract class AbstractBlockingDatabase implements Database {
  protected Configuration config;
  protected Collection<KeyFunction> functions;
  protected Map<String, Record> idmap;
  protected Map<KeyFunction, NavigableMap> func_to_map;

  // config
  protected int window_size;

  public AbstractBlockingDatabase() {
    this.functions = new ArrayList();
    this.func_to_map = new HashMap();
    this.window_size = 5;
  }

  public void setConfiguration(Configuration config) {
    this.config = config;
  }

  public void setOverwrite(boolean overwrite) {
  }

  /**
   * Sets the minimum number of records to gather from blocks on each
   * side of the start block. If the start block has more records than
   * twice the window size no neighbouring blocks are searched.
   * Setting window_size = 0 disables searching of neighbouring
   * blocks.
   */
  public void setWindowSize(int window_size) {
    this.window_size = window_size;
  }

  /**
   * Sets the key functions used for blocking.
   */
  public void setKeyFunctions(Collection<KeyFunction> functions) {
    this.functions = functions;
  }

  public Collection<KeyFunction> getKeyFunctions() {
    return functions;
  }

  protected void indexById(Record record) {
    for (Property idprop : config.getIdentityProperties())
      for (String id : record.getValues(idprop.getName()))
        idmap.put(id, record);
  }

  public Record findRecordById(String id) {
    return idmap.get(id);
  }

  public Collection<Record> findCandidateMatches(Record record){
    return findCandidateMatches(record,null);
  }

  public Collection<Record> findCandidateMatches(Record record,Collection<Filter> filters) {
    Collection<Record> candidates = new HashSet(); //ArrayList();

    for (KeyFunction keyfunc : functions) {
      NavigableMap<String, Object> blocks = getBlocks(keyfunc);
      String key = keyfunc.makeKey(record);
      // System.out.println("key: '" + key + "'");

      // look up the first block
      Map.Entry<String, Object> start = blocks.ceilingEntry(key);
      Map.Entry<String, Object> entry = start;
      if (start == null)
        continue;

      // add all records from this block
      int added = addBlock(candidates, start);
      // System.out.println("entry '" + entry.getKey() + "' " + added);
      // System.out.println("start: " + start.getValue() + " " + added);
      if (added > window_size * 2)
        continue; // we can't add more candidates from this key function

      // then we navigate downwards from the key
      int added_this_way = added / 2;
      entry = blocks.lowerEntry(entry.getKey());
      while (entry != null && added_this_way < window_size) {
        // System.out.println("entry low: " + entry.getValue() + " " + added_this_way);
        added_this_way += addBlock(candidates, entry);
        // System.out.println("entry '" + entry.getKey() + "' " + entry.getValue().size());

        entry = blocks.lowerEntry(entry.getKey());
      }

      // then we navigate upwards from the key
      added_this_way = added / 2;
      entry = blocks.higherEntry(start.getKey());
      while (entry != null && added_this_way < window_size) {
        // System.out.println("entry high: " + entry.getValue() + " " + added_this_way);
        added_this_way += addBlock(candidates, entry);
        // System.out.println("entry '" + entry.getKey() + "' " + entry.getValue().size());

        entry = blocks.higherEntry(entry.getKey());
      }
    }

    return filter(candidates,filters);
  }


  public void commit() {
  }

  public void close() {
  }

  public NavigableMap getBlocks(KeyFunction keyfunc) {
    NavigableMap map = func_to_map.get(keyfunc);
    if (map == null) {
      map = makeMap(keyfunc);
      func_to_map.put(keyfunc, map);
    }
    return map;
  }

  // --- extension points

  // must also implement index(Record)

  // returns number of records added
  protected abstract int addBlock(Collection<Record> candidates,
                                  Map.Entry block);

  protected abstract NavigableMap makeMap(KeyFunction keyfunc);
}
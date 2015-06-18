package storm.trident.state.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.ObserverZooKeeperServer;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.TransactionalMap;
import storm.trident.tuple.TridentTupleView;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class MysqlState<T> implements IBackingMap<T> {

	private Connection connection;
	private MysqlStateConfig config;
	private static final Logger logger = Logger.getLogger(MysqlState.class);

	MysqlState(final MysqlStateConfig config) {
		this.config = config;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(config.getUrl());
		} catch (final SQLException | ClassNotFoundException ex) {
			logger.error("Failed to establish DB connection", ex);
		}
	}

	/**
	 * factory method for the factory
	 * 
	 * @param config
	 * @return
	 */
	public static Factory newFactory(final MysqlStateConfig config) {
		return new Factory(config);
	}

	static class MyArrayList<T> extends AbstractList<T> {

		private int currIndex = 0;
	    private final T[] a;

	    MyArrayList(T[] array) {
	        a = array;
	    }

	    @Override
	    public T get(int index) {
	        return a[index];
	    }

	    @Override
	    public T set(int index, T element) {
	        T oldValue = a[index];
	        a[index] = element;
	        return oldValue;
	    }

	    
	    
	    @Override
	    public boolean add(T element){
	    	a[currIndex] = element;
	    	currIndex++;
	    	return true;
	    }
	    
	    @Override
	    public int size() {
	        return a.length;
	    }

	    @Override
	    public Object[] toArray() {
	        return (Object[]) a.clone();
	    }

	    public static void main(String[] args) {
	        String[] arr = new String[2];
	        MyArrayList<String> list = new MyArrayList<String>(arr);
	        list.add("seven");
	        System.out.println(list);
	    }
	}

		
	public static void main(String[] args) {
		List<Object> l1 =  new ArrayList<>();
		List<Object> l2 =  new ArrayList<>();
		Object[] arr = new Object[4];
        MyArrayList<Object> list = new MyArrayList<Object>(arr);
		
        list.add(1);
        list.add("1");
        list.add(new Long(1));
        list.add(new Long(1));
        
		l1.add(1);
		l1.add("1");
		l1.add(new Long(1));
		l1.add(new Long(1));
		
		l2.add(1);
		l2.add("1");
		l2.add(new Long(1));
		l2.add(new Integer(1));
		
		System.out.println(l1.hashCode());
		System.out.println(l2.hashCode());
		
		
		Map<List<Object>, String> m = new HashMap<>();
		m.put(l1, "aaaa");
		
		String string = m.get(l2);
		String string2 = m.get(list);
		
		
		System.out.println("string " + string);
		System.out.println("string2 " + string2);
		
		if(l1.equals(l2)){
			System.out.println("esittt");
		}else{
			System.out.println("esittt degil");
		}
				
	}
	/**
	 * multiget implementation for mysql
	 * 
	 */
	@Override
	@SuppressWarnings({"unchecked","rawtypes"})
	public List<T> multiGet(final List<List<Object>> keys) {
		final List<List<List<Object>>> partitionedKeys = Lists.partition(keys, config.getBatchSize());
		final List<T> result = new ArrayList<>();
		for (final List<List<Object>> pkeys : partitionedKeys) {
			// build a query using select key1, keys2, ..., val1, val2, ..., [txid], [prev_val1], ... FROM table WHERE (key1 = ? AND keys = ? ...) OR ...
			final StringBuilder queryBuilder = new StringBuilder().append("SELECT ")
				.append(buildColumns())
				.append(" FROM ")
				.append(config.getTable())
				.append(" WHERE ")
				.append(buildKeyQuery(pkeys.size()));
			
			final Map<String, List<Object>> queryResults = query(queryBuilder.toString(), pkeys);
			

			for (List<Object> key : pkeys) {
				
				String columnKey = "";
				for(Object keyPart: key) {
					columnKey += keyPart + "-";
				}
				
				List<Object> values =  queryResults.get(columnKey);
				if (values == null) {
					result.add(null);
				} else {
					switch (config.getType()) {
					case OPAQUE: // partition the values list into 3 values [current], [txid], [prev]
						OpaqueValue opaqueValue = new OpaqueValue((Long) values.get(config.getValueColumns().length), // txid
								values.subList(0, config.getValueColumns().length), // curr values
								values.subList(config.getValueColumns().length, values.size()));
						result.add((T)opaqueValue);
						break;
					case TRANSACTIONAL:
						Object object = values.get(config.getValueColumns().length);
						TransactionalValue transactionalValue = new TransactionalValue( object ==null ? null : Long.valueOf(values.get(config.getValueColumns().length).toString()), // txid
								values.subList(0, config.getValueColumns().length));
						result.add((T)transactionalValue);
						break;
					case NON_TRANSACTIONAL:
						break;
					default:
						result.addAll((List<T>)values);
						break;
					}
				}
			}
				
			}
		
		
		return result;
	}

	/**
	 * multiget implementation for mysql
	 * 
	 */
	@Override
	public void multiPut(final List<List<Object>> keys, final List<T> values) {
		// partitions the keys and the values and run it over every one
		final Iterator<List<List<Object>>> partitionedKeys = Lists.partition(keys, config.getBatchSize()).iterator();
		final Iterator<List<T>> partitionedValues = Lists.partition(values, config.getBatchSize()).iterator();
		while (partitionedKeys.hasNext() && partitionedValues.hasNext()) {
			final List<List<Object>> pkeys = partitionedKeys.next();
			final List<T> pvalues = partitionedValues.next();
			// build a query insert into table(key1, key2, ..., value1, value2, ... , [txid], [prev_val1], ...) values (?,?,...), ... ON DUPLICATE KEY UPDATE value1 = VALUES(value1), ...
			// how many params per row of data
			// opaque => keys + 2 * vals + 1
			// transactional => keys + vals + 1
			// non-transactional => keys + vals
			int paramCount = 0;
			switch (config.getType()) {
			case OPAQUE:
				paramCount += config.getValueColumns().length;
			case TRANSACTIONAL:
				paramCount += 1;
			default:
				paramCount += (config.getKeyColumns().length + config.getValueColumns().length);
			}
			final StringBuilder queryBuilder = new StringBuilder().append("INSERT INTO ")
				.append(config.getTable())
				.append("(")
				.append(buildColumns())
				.append(") VALUES ")
				.append(Joiner.on(",").join(repeat("(" + Joiner.on(",").join(repeat("?", paramCount)) + ")", pkeys.size())))
				.append(" ON DUPLICATE KEY UPDATE ")
				.append(Joiner.on(",").join(Lists.transform(getValueColumns(), new Function<String, String>() {
					@Override
					public String apply(final String col) {
//						return col + " = " + col + " +  VALUES(" + col + ")";
						//TODO: burada memoryde aggregate edilmis olan value geliyor , eger topology ölür ve bastan baslarsa 0 dan baslamis oluyor.
						return col + " = VALUES(" + col + ")";
					}
				}))); // for every value column, constructs "valcol = VALUE(valcol)", joined by commas
			// run the update
			final List<Object> params = flattenPutParams(pkeys, pvalues);
			PreparedStatement ps = null;
			int i = 0;
			try {
				ps = connection.prepareStatement(queryBuilder.toString());
				for (final Object param : params) {
					ps.setObject(++i, param);
				}
				ps.execute();
			} catch (final SQLException ex) {
				logger.error("Multiput update failed", ex);
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (SQLException ex) {
						// don't care
					}
				}
			}
			logger.debug(String.format("%1$d keys flushed", pkeys.size()));
		}
	}

	private String buildColumns() {
		final List<String> cols = Lists.newArrayList(config.getKeyColumns()); // the columns for the composite unique key
		cols.addAll(getValueColumns());
		return Joiner.on(",").join(cols);
	}

	private String buildKeyQuery(final int n) {
		final String single = "(" + Joiner.on(" AND ").join(Lists.transform(Arrays.asList(config.getKeyColumns()), new Function<String, String>() {
			@Override
			public String apply(final String field) {
				return field + " = ?";
			}
		})) + ")";
		return Joiner.on(" OR ").join(repeat(single, n));
	}

	private List<String> getValueColumns() {
		final List<String> cols = Lists.newArrayList(config.getValueColumns()); // the columns storing the values
		if (StateType.OPAQUE.equals(config.getType()) || StateType.TRANSACTIONAL.equals(config.getType())) {
			cols.add("txid");
		}
		if (StateType.OPAQUE.equals(config.getType())) {
			cols.addAll(Lists.transform(Arrays.asList(config.getValueColumns()), new Function<String, String>() {
				@Override
				public String apply(final String field) {
					return "prev_" + field;
				}
			})); // the prev_* columns
		}
		return cols;
	}
	
	/**
	 * run the multi get query, passing in the list of keys and returning key tuples mapped to value tuples
	 * 
	 * @param sql
	 * @param keys
	 * @return
	 */
	private Map<List<Object>, List<Object>> query2(final String sql, final List<List<Object>> keys) {
		final Map<List<Object>, List<Object>> result = new HashMap<>();
		PreparedStatement ps = null;
		int i = 0;
		try {
			ps = connection.prepareStatement(sql);
			for (final List<Object> key : keys) {
				for (final Object keyPart : key) {
					ps.setObject(++i, keyPart);
				}
			}
			final ResultSet rs = ps.executeQuery();
			final Function<String, Object> rsReader = new Function<String, Object>() {
				@Override
				public Object apply(final String column) {
					try {
						return rs.getObject(column);
					} catch (final SQLException sqlex) {
						return null;
					}
				}
			};
			final List<String> keyColumns = Arrays.asList(config.getKeyColumns());
			final List<String> valueColumns = getValueColumns();
			while (rs.next()) {
				List<Object> transform = Lists.transform(keyColumns, rsReader);
				result.put(transform, Lists.transform(valueColumns, rsReader));
			}
			rs.close();
		} catch (final SQLException ex) {
			logger.error("multiget query failed", ex);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException ex) {
					// don't care
				}
			}
		}
		return result;
	}
	
	@SuppressWarnings({"unchecked","rawtypes"})
	public List<T> multiGet3(final List<List<Object>> keys) {
		final List<List<List<Object>>> partitionedKeys = Lists.partition(keys, config.getBatchSize());
		final List<T> result = new ArrayList<>();
		for (final List<List<Object>> pkeys : partitionedKeys) {
			// build a query using select key1, keys2, ..., val1, val2, ..., [txid], [prev_val1], ... FROM table WHERE (key1 = ? AND keys = ? ...) OR ...
			final StringBuilder queryBuilder = new StringBuilder().append("SELECT ")
				.append(buildColumns())
				.append(" FROM ")
				.append(config.getTable())
				.append(" WHERE ")
				.append(buildKeyQuery(pkeys.size()));
			final Map<List<Object>, List<Object>> queryResults = query2(queryBuilder.toString(), pkeys);
			// build the value list by ordering based on the input keys and looking up the query results, transform to transactional and opaque values as needed
			result.addAll(Lists.transform(pkeys, new Function<List<Object>, T>() {
				@Override
				public T apply(final List<Object> key) {
					final List<Object> values = queryResults.get(key);
					if (values == null) {
						return null;
					} else {
						switch (config.getType()) {
						case OPAQUE: // partition the values list into 3 values [current], [txid], [prev]
							return (T) new OpaqueValue((Long) values.get(config.getValueColumns().length), // txid
								values.subList(0, config.getValueColumns().length), // curr values
								values.subList(config.getValueColumns().length, values.size())); // prev values
						case TRANSACTIONAL:
							return (T) new TransactionalValue((Long) values.get(config.getValueColumns().length), // txid
								values.subList(0, config.getValueColumns().length)); // curr values
						default:
							return (T) values;
						}
					}
				}
			}));
			logger.debug(String.format("%1$d keys retrieved", pkeys.size()));
		}
		return result;
	}

	/**
	 * run the multi get query, passing in the list of keys and returning key tuples mapped to value tuples
	 * 
	 * @param sql
	 * @param keys
	 * @return
	 */
	private Map<String, List<Object>> query(final String sql, final List<List<Object>> keys) {
		Map<String, List<Object>> result = new HashMap<>();
		PreparedStatement ps = null;
		int i = 0;
		try {
			ps = connection.prepareStatement(sql);
			for (final List<Object> key : keys) {
				for (final Object keyPart : key) {
					ps.setObject(++i, keyPart);
				}
			}
			ResultSet rs = ps.executeQuery();
			List<String> keyColumns = Arrays.asList(config.getKeyColumns());
			
			List<String> valueColumns = getValueColumns();
			while (rs.next()) {
				String columnKey = "";
				for (String colm : keyColumns) {
					columnKey += rs.getObject(colm) + "-";
				}
				List<Object> transform2 = new ArrayList<>();
				for (String colm : valueColumns) {
					transform2.add(rs.getObject(colm));
				}
				
				
				result.put(columnKey, transform2);
			}
			rs.close();
		} catch (final SQLException ex) {
			logger.error("multiget query failed", ex);
		} finally {
			if (ps != null) {
				try {
					
					ps.close();
					
				} catch (SQLException ex) {
					// don't care
				}
			}
		}
		return result;
	}

	@SuppressWarnings("rawtypes")
	private List<Object> flattenPutParams(final List<List<Object>> keys, final List<T> values) {
		final List<Object> flattenedRows = new ArrayList<>();
		for (int i = 0; i < keys.size(); i++) {
			flattenedRows.addAll(keys.get(i));
			switch (config.getType()) {
			case OPAQUE:
				flattenedRows.addAll(valueToParams(((OpaqueValue) values.get(i)).getCurr()));
				flattenedRows.add(((OpaqueValue) values.get(i)).getCurrTxid());
				flattenedRows.addAll(valueToParams(((OpaqueValue) values.get(i)).getPrev()));
				break;
			case TRANSACTIONAL:
				flattenedRows.addAll(valueToParams(((TransactionalValue) values.get(i)).getVal()));
				flattenedRows.add(((TransactionalValue) values.get(i)).getTxid());
				break;
			default:
				flattenedRows.addAll(valueToParams(values.get(i)));
			}
		}
		return flattenedRows;
	}

	@SuppressWarnings("unchecked")
	private List<Object> valueToParams(final Object value) {
		if (!(value instanceof List)) {
			return repeat(value, config.getValueColumns().length);
		} else {
			return (List<Object>) value;
		}
	}

	private <U> List<U> repeat(final U val, final int count) {
		final List<U> list = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			list.add(val);
		}
		return list;
	}

	@SuppressWarnings("serial")
	static class Factory implements StateFactory {
		private MysqlStateConfig config;

		Factory(final MysqlStateConfig config) {
			this.config = config;
		}

		@Override
		@SuppressWarnings({"rawtypes","unchecked"})
		public State makeState(final Map conf, final IMetricsContext context, final int partitionIndex, final int numPartitions) {
			final CachedMap map = new CachedMap(new MysqlState(config), config.getCacheSize());
//			final CachedMap map = new CachedMap(new MysqlState(config), 0);
			switch (config.getType()) {
			case OPAQUE:
				return OpaqueMap.build(map);
			case TRANSACTIONAL:
				return TransactionalMap.build(map);
			default:
				return NonTransactionalMap.build(map);
			}
		}
	}
}

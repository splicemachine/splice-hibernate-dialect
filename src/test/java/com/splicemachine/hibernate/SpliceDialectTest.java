package com.splicemachine.hibernate;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class SpliceDialectTest {

	private static class LocalSpliceMachineDialect extends SpliceMachineDialect {
	}

	@Test
	public void testInsertLimitClause() {
		final int limit = 50;
		final String input = "select * from tablename t where t.cat = 5";
		final String expected = "select * from tablename t where t.cat = 5 fetch first " + limit + " rows only";

		final String actual = new LocalSpliceMachineDialect().getLimitString( input, 0, limit );
		assertEquals( expected, actual );
	}

	@Test
	public void testInsertLimitWithOffsetClause() {
		final int limit = 50;
		final int offset = 200;
		final String input = "select * from tablename t where t.cat = 5";
		final String expected = "select * from tablename t where t.cat = 5 offset " + offset + " rows fetch next " + limit + " rows only";

		final String actual = new LocalSpliceMachineDialect().getLimitString( input, offset, limit );
		assertEquals( expected, actual );
	}

	@Test
	public void testInsertLimitWithForUpdateClause() {
		final int limit = 50;
		final int offset = 200;
		final String input = "select c11 as col1, c12 as col2, c13 as col13 from t1 for update of c11, c13";
		final String expected = "select c11 as col1, c12 as col2, c13 as col13 from t1 offset " + offset
				+ " rows fetch next " + limit + " rows only for update of c11, c13";

		final String actual = new LocalSpliceMachineDialect().getLimitString( input, offset, limit );
		assertEquals( expected, actual );
	}

	@Test
	public void testInsertLimitWithWithClause() {
		final int limit = 50;
		final int offset = 200;
		final String input = "select c11 as col1, c12 as col2, c13 as col13 from t1 where flight_id between 'AA1111' and 'AA1112' with rr";
		final String expected = "select c11 as col1, c12 as col2, c13 as col13 from t1 where flight_id between 'AA1111' and 'AA1112' offset " + offset
				+ " rows fetch next " + limit + " rows only with rr";

		final String actual = new LocalSpliceMachineDialect().getLimitString( input, offset, limit );
		assertEquals( expected, actual );
	}

	@Test
	public void testInsertLimitWithForUpdateAndWithClauses() {
		final int limit = 50;
		final int offset = 200;
		final String input = "select c11 as col1, c12 as col2, c13 as col13 from t1 where flight_id between 'AA1111' and 'AA1112' for update of c11,c13 with rr";
		final String expected = "select c11 as col1, c12 as col2, c13 as col13 from t1 where flight_id between 'AA1111' and 'AA1112' offset " + offset
				+ " rows fetch next " + limit + " rows only for update of c11,c13 with rr";

		final String actual = new LocalSpliceMachineDialect().getLimitString( input, offset, limit );
		assertEquals( expected, actual );
	}
}
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A data frame holds a matrix of data, with the difference that column have
 * names rather than indices.
 * 
 * It supports a number of useful operations to manipulate the data that can
 * transform and aggregate the data stored in the matrix.
 * 
 * Note that the order of the column is fixed.
 * 
 * @author Paul Bouman
 *
 * @param <E> the type the entry values stored in this data vector
 */
public interface DataFrame<E> extends Iterable<DataVector<E>>
{

	/**
	 * Defauft number of characters to use for the width of a single column when
	 * formatting a DataFrame or DataVector object.
	 */
	public static int DEFAULT_FORMAT_WIDTH = 12;

	/**
	 * The number of rows stored in this matrix
	 * 
	 * @return the number of rows
	 */
	public int getRowCount();

	/**
	 * The number of columns stored in this matrix
	 * 
	 * @return the number of columns
	 */
	public int getColumnCount();

	/**
	 * The names of the columns stored in this matrix
	 * 
	 * @return a list of column names
	 */
	public List<String> getColumnNames();

	/**
	 * Sets a value to a particular entry in the data frame.
	 * 
	 * @param rowIndex the row index of the entry
	 * @param colName  the name of the column in which the entry is stored
	 * @param value    the new value of the entry
	 * @throws IndexOutOfBoundsException if the row index is illegal
	 * @throws IllegalArgumentException  if the column name is illegal
	 */
	public void setValue(int rowIndex, String colName, E value)
			throws IndexOutOfBoundsException, IllegalArgumentException;

	/**
	 * Retrieve the value associated with a given entry in the data frame
	 * 
	 * @param rowIndex the row index of the entry
	 * @param colName  the name of the column in which the entry is stored
	 * @return the value currently stored at the entry
	 * @throws IndexOutOfBoundsException if the row index is illegal
	 * @throws IllegalArgumentException  if the column name is illegal
	 */
	public E getValue(int rowIndex, String colName) throws IndexOutOfBoundsException, IllegalArgumentException;

	/**
	 * Produces a data vector for a particular row in the data frame
	 * 
	 * @param rowIndex the row index of the entry
	 * @return a data vector derived from a row in the data frame
	 * @throws IndexOutOfBoundsException if the row index is illegal
	 */
	public DataVector<E> getRow(int rowIndex) throws IndexOutOfBoundsException;

	/**
	 * Produces a data vector for a particular column in the data frame
	 * 
	 * @param colName the name of the column
	 * @return a data vector derived from a column in the data frame
	 * @throws IllegalArgumentException if the column name is illegal
	 */
	public DataVector<E> getColumn(String colName) throws IllegalArgumentException;

	/**
	 * Produces a list of data vectors derived from every row in the matrix
	 * 
	 * @return a list of data vectors
	 */
	public List<DataVector<E>> getRows();

	/**
	 * Produces a list of data vectors derived from every column in the matrix
	 * 
	 * @return a list of data vectors
	 */
	public List<DataVector<E>> getColumns();

	/**
	 * Copies the values stored in this data frame into a larger data frame, and
	 * returns the result.
	 * 
	 * @param additionalRows the number of rows to add to the new data frame
	 * @param newCols        the names of the column to add to the new data frame.
	 * @return the expanded resulting data frame
	 * @throws IllegalArgumentException the number of additional rows in negative or
	 *                                  column names are duplicated
	 */
	public DataFrame<E> expand(int additionalRows, List<String> newCols) throws IllegalArgumentException;

	/**
	 * Produces a smaller data frame that only contains the columns with names that
	 * occur in the provided set of column names
	 * 
	 * @param retainColumns the names of column that should be retained
	 * @return a smaller data frame
	 * @throws IllegalArgumentException if one of the column names is illegal
	 */
	public DataFrame<E> project(Collection<String> retainColumns) throws IllegalArgumentException;

	/**
	 * Produces a smaller data frame the only keeps the rows that are accepted by
	 * the predicate
	 * 
	 * @param rowFilter a predicate that can indicate whether or not a row should be
	 *                  maintained
	 * @return a smaller data frame
	 */
	public DataFrame<E> select(Predicate<DataVector<E>> rowFilter);

	/**
	 * Produces a larger data frame with one additional column. The values stored in
	 * this column are computed using a give function that is applied to each row
	 * currently in the data frame
	 * 
	 * @param columnName the name of the new column
	 * @param function   the function to apply to each row
	 * @return the resulting data frame
	 */
	public DataFrame<E> computeColumn(String columnName, Function<DataVector<E>, Double> function);

	/**
	 * Summarize each column using a given BinaryOperator using a reduce action. The
	 * result is produces as a data vector.
	 * 
	 * @param name            the name of the resulting data vector
	 * @param summaryFunction the binary operator that should be used to reduce the
	 *                        values in each column
	 * @return a data vector with the result for each column
	 */
	public DataVector<E> summarize(String name, BinaryOperator<E> summaryFunction);

	/**
	 * Obtains an object that can be used to perform statistical analysis on this
	 * data frame.
	 * 
	 * @return a DataFrameStatistics object linked to this data frame
	 * @throws UnsupportedOperationException if no statistical functionalities are
	 *                                       available
	 */
	public default DataFrameStatistics statistics() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Obtains an object that can be used to perform visualization of the data in
	 * this data frame.
	 * 
	 * @return a DataFramePlotting object linked to this data frame
	 * @throws UnsupportedOperationException if no plotting functionalities are
	 *                                       available
	 */
	public default DataFramePlotting plotting() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Convenience method that expands the data frame with passing an explicit list
	 * of new column names
	 * 
	 * @param additionalRows the number of new rows
	 * @param newCols        the names of the new columns to add
	 * @return an exapneded data frame
	 * @throws IllegalArgumentException if some of the row names clash
	 */
	public default DataFrame<E> expand(int additionalRows, String... newCols) throws IllegalArgumentException
	{
		return expand(additionalRows, Arrays.asList(newCols));
	}

	/**
	 * Produces a smaller data frame that only contains the columns with names that
	 * occur in the provided set of column names
	 * 
	 * @param retainColumns the names of column that should be retained
	 * @return a smaller data frame
	 * @throws IllegalArgumentException if one of the column names is illegal
	 */
	public default DataFrame<E> project(String... retainCols) throws IllegalArgumentException
	{
		return project(Arrays.asList(retainCols));
	}

	/**
	 * Expands the data frame to have a number of additional rows
	 * 
	 * @param additionalRows the number of rows to add
	 * @return a newly created expanded version of this data frame
	 */
	public default DataFrame<E> expandRows(int additionalRows)
	{
		if (additionalRows < 0)
		{
			throw new IllegalArgumentException("Number of rows should be positive");
		}
		return expand(additionalRows, Collections.emptyList());
	}

	/**
	 * Expands the data frame to have additional columns
	 * 
	 * @param newCols the names of the new columns
	 * @return a newly created expanded data frame
	 */
	public default DataFrame<E> expandColumns(List<String> newCols)
	{
		Set<String> uniqueNames = new HashSet<>(newCols);
		if (uniqueNames.size() != newCols.size() || uniqueNames.removeAll(getColumnNames()))
		{
			throw new IllegalArgumentException("Duplicate columns detected");
		}
		return expand(0, newCols);
	}

	/**
	 * Creates a new data frame from the concatenation of this data frame.
	 * 
	 * This requires that the number of rows in the two frames are equal.
	 * 
	 * @param other the data frame to concatenate to this data frame
	 * @return a newly created data frame containing both the data from this and the
	 *         other data frame
	 * @throws IllegalArgumentException if the number of rows do no match
	 */
	public default DataFrame<E> concat(DataFrame<E> other) throws IllegalArgumentException
	{
		if (getRowCount() != other.getRowCount())
		{
			throw new IllegalArgumentException("Can only concatenate dataframes with equal numbers of rows");
		}
		DataFrame<E> expanded = expand(0, other.getColumnNames());
		for (String colName : other.getColumnNames())
		{
			for (int i = 0; i < getRowCount(); i++)
			{
				expanded.setValue(i, colName, other.getValue(i, colName));
			}
		}
		return expanded;
	}

	/**
	 * Prints the contents of this data frame to System.out using a default column
	 * width
	 */
	public default void print()
	{
		System.out.println(formatMatrix(DEFAULT_FORMAT_WIDTH));
	}

	/**
	 * Formats the entries stored in this data frame to a fixed with using the
	 * String.format() method. The display is row-based: the first line contains all
	 * column names, then the second line contains the data from the first rows
	 * etcetera.
	 * 
	 * @param colWidth the number of character to use for a single column
	 * @return a string representation of this data frame
	 */
	public default String formatMatrix(int colWidth)
	{
		String fmt = "%-" + colWidth + "." + colWidth + "s";
		StringBuilder sb = new StringBuilder();
		sb.append(String.format(fmt, ""));
		List<String> colNames = getColumnNames();
		for (String colName : colNames)
		{
			sb.append(" ");
			sb.append(String.format(Locale.ROOT, fmt, colName));
		}
		sb.append("\n");
		for (int i = 0; i < getRowCount(); i++)
		{
			sb.append(String.format(Locale.ROOT, fmt, "row_" + i));
			for (String colName : colNames)
			{
				sb.append(" ");
				sb.append(String.format(Locale.ROOT, fmt, getValue(i, colName)));
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	@Override
	public default Iterator<DataVector<E>> iterator()
	{
		return getRows().iterator();
	}
}

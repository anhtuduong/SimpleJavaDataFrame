import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A data vector represents a vector of data of a certain type. In our DataFrame
 * library, a DataVector is named and thus has an associated name. For column
 * vectors, this should be the name associated with the name of the column, for
 * row vectors this should be "row_0" for the first row, "row_1" for the second
 * row, etctera.
 * 
 * The entries in this vector have names associated with them. For a vector
 * derived from a row in the data frame, these are typically the column names.
 * 
 * @author Paul Bouman
 *
 * @param <E> the type the entry values stored in this data vector
 */
public interface DataVector<E>
{

	/**
	 * Provides the name of this data vector For vectors derived from a column, this
	 * is typically the column name. For vectors derived from a row, it is typically
	 * "row_0", "row_1", etcetera.
	 * 
	 * @return
	 */
	public String getName();

	/**
	 * Gives a list of the names of the entries in this vector. For a vector derived
	 * from a column, these are typically "row_0", "row_1", etctera. For a vector
	 * derived from a row, these are the names of the columns
	 * 
	 * @return the names of the entries of this vector
	 */
	public List<String> getEntryNames();

	/**
	 * Getter for the value associated with an entry that has a given entry name.
	 * 
	 * @param entryName the name of the entry to extract
	 * @return the value of the entry with the entryName
	 */
	public E getValue(String entryName);

	/**
	 * Obtain a list of all the values of the entries in this vector.
	 * 
	 * @return a list of all values
	 */
	public List<E> getValues();

	/**
	 * Produces a map that contains key-value pairs of the entries in this vector.
	 * 
	 * @return a map with the entries of this vector as key-value pairs
	 */
	public Map<String, E> asMap();

	/**
	 * Formats the entries stored in this vector to a fixed with using the
	 * String.format() method. The display is always row-based: on the first line
	 * all entry names are displayed, while on the second line all associated values
	 * are displayed.
	 * 
	 * @param colWidth the width in number of characters for a single entry
	 * @return a formatted string with the names and values of the entries
	 */
	public default String formatRow(int colWidth)
	{
		String fmt = "%-" + colWidth + "." + colWidth + "s";
		List<Object> rowObjects = new ArrayList<>();
		rowObjects.add(getName());
		rowObjects.addAll(getValues());
		return rowObjects.stream()
				.map(o -> String.format(Locale.ROOT, fmt, o))
				.collect(Collectors.joining(" "));
	}

	/**
	 * Prints a formatted version of this vector to System.out using the default
	 * width defined in the DataFrame interface.
	 */
	public default void print()
	{
		System.out.println(formatRow(DataFrame.DEFAULT_FORMAT_WIDTH));
	}

}

import javax.xml.crypto.Data;
import javax.xml.stream.XMLOutputFactory;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

public class DoubleDataFrame implements DataFrame<Double>
{
    /* Anh Tu's note:
    * List of list is 2 dimensions data, equivalent to [][]
    * for example:
    * list = List { list0 {1,2,3},
    *               list1 {4,5,6},
    *               list2 {7,8,9}
    *              }
    * array[][] = { {1,2,3},
    *               {4,5,6},
    *               {7,8,9}
    *              }
    * list.get(1).get(2) equals to array[1][2]
    * which means: first get the row index 1
    * which is the list {4,5,6}. Then get the column index 2,
    * which is the number 6.
    */
    private List<List<Double>> data;

    /* Anh Tu's note:
    * A Map<A, B> is like a dictionary that connect
    * a key (of datatype A) and a value (of type B).
    * In this case, in order to find the column index,
    * the fastest possible will be creating a Map<String, Integer>
    * that assign column names to index numbers. For example:
    * map.put("columnA", 0) <-- Assign the key "columnA" to value 0
    * map.put("columnB", 1) <-- Assign the key "columnB" to value 1
    * ...
    * Whenever we need to find the number associated with "columnB", we call
    * map.get("columnB") <-- the result will be 1
    * */
    private Map<String, Integer> columnNamesMap;

    private static final double DEFAULT_DATA = 0.0;

    /*
    * Constructor: initialize data
    */
    public DoubleDataFrame(List<String> columnNames, double [][] data) {
        // Map the column names with the indexes
        this.columnNamesMap = new LinkedHashMap<>();
        int numberOfIndexes = columnNames.size();
        mapColumnNamesWithIndexes(this.columnNamesMap, columnNames, numberOfIndexes);

        // Convert data array to lists
        this.data = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            // data.length will return the length of the row
            // because imagine the array with 2 dimensions:
            // data[row][column]
            List<Double> rowData = new ArrayList<>();
            for (int j = 0; j < data[i].length; j++) {
                // data[i].length will return the length
                // of the column
                rowData.add(data[i][j]);
            }
            this.data.add(rowData);
        }
    }

    /*
     * Copy constructor
     */
    public DoubleDataFrame(DoubleDataFrame other) {
        // Copy column name map
        this.columnNamesMap = new LinkedHashMap<>();
        this.columnNamesMap.putAll(other.getColumnNamesMap());
        // Copy data
        this.data = new ArrayList<>();
        for (List<Double> rowData : other.getData()) {
            this.data.add(new ArrayList<>(rowData));
        }
    }

    /*
    * Constructor: construct from list of rows or columns
    */
    public DoubleDataFrame(List<DataVector<Double>> dataVectorList, boolean isRow) {
        // If input is a list of row vectors
        if (isRow) {
            constructFromListOfRows(dataVectorList);
        }
        // If input is a list of column vectors
        else {
            constructFromListOfColumns(dataVectorList);
        }
    }

    private void constructFromListOfRows(List<DataVector<Double>> dataVectorList) {
        // Map the column names with the indexes
        this.columnNamesMap = new LinkedHashMap<>();
        if (!dataVectorList.isEmpty()) {
            DataVector<Double> dataVector = dataVectorList.get(0);
            List<String> columnNames = dataVector.getEntryNames();
            int numberOfIndexes = columnNames.size();
            mapColumnNamesWithIndexes(this.columnNamesMap, columnNames, numberOfIndexes);
        }

        // Copy data
        this.data = new ArrayList<>();
        for (DataVector<Double> dataVector : dataVectorList) {
            List<Double> rowData = dataVector.getValues();
            this.data.add(rowData);
        }
    }

    private void constructFromListOfColumns(List<DataVector<Double>> dataVectorList) {
        // Initialize data and map
        this.data = new ArrayList<>();
        this.columnNamesMap = new LinkedHashMap<>();
        boolean isFirstColumn = true;

        // Iterate each column vector
        for (int i = 0; i < dataVectorList.size(); i++) {
            DataVector<Double> dataVector = dataVectorList.get(i);
            // Map the column name with index
            String columnName = dataVector.getName();
            this.columnNamesMap.put(columnName, i);
            // Copy data
            List<Double> columnData = dataVector.getValues();
            for (int k = 0; k < columnData.size(); k++) {
                Double data = columnData.get(k);
                if (isFirstColumn) {
                    this.data.add(new ArrayList<>());
                }
                List<Double> rowData = this.data.get(k);
                rowData.add(data);
            }
            isFirstColumn = false;
        }
    }

    private void mapColumnNamesWithIndexes(Map<String, Integer> columnNamesMap, List<String> columnNames, int numberOfIndexes) {
        for (int i = 0; i < numberOfIndexes; i++) {
            // Iterate each column name
            String key = columnNames.get(i);
            Integer value = i;
            // and connect column name with the index
            columnNamesMap.put(key, value);
        }
    }

    @Override
    public int getRowCount() {
        return this.data.size();
    }

    @Override
    public int getColumnCount() {
        if (getRowCount() > 0) {
            return this.data.get(0).size();
        }
        return 0;
    }

    @Override
    public List<String> getColumnNames() {
        // columnNamesMap.keySet() will return
        // the set of {"columnA", "columnB", ...}
        // Cast it to a list
        return new ArrayList<>(this.columnNamesMap.keySet());
    }

    @Override
    public void setValue(int rowIndex, String colName, Double value) throws IndexOutOfBoundsException, IllegalArgumentException {
        if (isValidRowIndex(rowIndex) && isValidColumnName(colName)) {
            int columnIndex = this.columnNamesMap.get(colName);
            this.data.get(rowIndex).set(columnIndex, value);
        }
    }

    @Override
    public Double getValue(int rowIndex, String colName) throws IndexOutOfBoundsException, IllegalArgumentException {
        Double result = null;
        if (isValidRowIndex(rowIndex) && isValidColumnName(colName)) {
            int columnIndex = this.columnNamesMap.get(colName);
            result = this.data.get(rowIndex).get(columnIndex);
        }
        return result;
    }

    private boolean isValidRowIndex(int rowIndex) {
        if (rowIndex < 0 || rowIndex > getRowCount()) {
            throw new IndexOutOfBoundsException("Invalid row index!");
        }
        return true;
    }

    private boolean isValidColumnName(String colName) {
        if (!this.columnNamesMap.containsKey(colName)) {
            String msg = "Column name " + colName + " not exists!";
            throw new IllegalArgumentException(msg);
        }
        return true;
    }

    @Override
    public DataVector<Double> getRow(int rowIndex) throws IndexOutOfBoundsException {
        DataVector<Double> result = null;
        if (isValidRowIndex(rowIndex)) {
            String rowName = "row_" + rowIndex;
            List<Double> rowData = new ArrayList<>(this.data.get(rowIndex));
            result = new DoubleDataVector(rowName, getColumnNames(), rowData);
        }
        return result;
    }

    @Override
    public DataVector<Double> getColumn(String colName) throws IllegalArgumentException {
        DataVector<Double> result = null;
        if (isValidColumnName(colName)) {
            // Create the row names
            List<String> rowNames = new ArrayList<>();
            for (int i = 0; i < this.data.size(); i++) {
                rowNames.add("row_" + i);
            }
            // Collect the column data
            List<Double> colData = new ArrayList<>();
            int colIndex = this.columnNamesMap.get(colName);
            for (List<Double> rowList : this.data) {
                Double value = rowList.get(colIndex);
                colData.add(value);
            }
            // Create DoubleDataVector result
            result = new DoubleDataVector(colName, rowNames, colData);
        }
        return result;
    }

    @Override
    public List<DataVector<Double>> getRows() {
        List<DataVector<Double>> result = new ArrayList<>();
        for (int i = 0; i < getRowCount(); i++) {
            result.add(getRow(i));
        }
        return result;
    }

    @Override
    public List<DataVector<Double>> getColumns() {
        List<DataVector<Double>> result = new ArrayList<>();
        for (String colName : getColumnNames()) {
            result.add(getColumn(colName));
        }
        return result;
    }

    @Override
    public DataFrame<Double> expand(int additionalRows, List<String> newCols) throws IllegalArgumentException {
        // Make a copy of this DoubleDataFrame object
        DoubleDataFrame result = new DoubleDataFrame(this);

        boolean isFirstRow = true;

        // Extend new column data
        for (List<Double> rowData : result.getData()) {
            // Iterate each new column name
            for (String newColName : newCols) {
                if (isFirstRow) {
                    // Check if it is already defined in the original data
                    boolean isDuplicated = result.getColumnNamesMap().containsKey(newColName);
                    if (isDuplicated) {
                        String msg = "Column " + newColName + " is already defined!";
                        throw new IllegalArgumentException(msg);
                    }
                    // If not defined yet
                    // Map the new column name with new index
                    int newColumnIndex = result.getColumnNames().size();
                    result.getColumnNamesMap().put(newColName, newColumnIndex);
                }

                // Extend the column data with default value 0.0
                rowData.add(DEFAULT_DATA);
            }
            isFirstRow = false;
        }

        // Extend new row lists
        if (additionalRows > 0) {
            int newTotalColumnCount = getColumnCount() + newCols.size();
            for (int i = 0; i < additionalRows; i++) {
                // Create new row list filled with 0.0
                List<Double> extendRowData = new ArrayList<>(Collections.nCopies(newTotalColumnCount, DEFAULT_DATA));
                // Add new row list to the extended DataFrame
                result.getData().add(extendRowData);
            }
        }

        return result;
    }

    @Override
    public DataFrame<Double> project(Collection<String> retainColumns) throws IllegalArgumentException {
        // Initialize a sorted map that connects the index with column name
        Map<Integer, String> sortedIndexColumnNameMap = new TreeMap<>();
        // Collect retain columns
        for (String columnName : retainColumns) {
            // Check if column name exists
            if (isValidColumnName(columnName)) {
                // Map the index with corresponded column name
                int columnIndex = this.columnNamesMap.get(columnName);
                sortedIndexColumnNameMap.put(columnIndex, columnName);
            }
        }
        // Now we can get the columns with original order
        // because the sorted map will sort automatically
        // by the index of the column
        List<DataVector<Double>> columnList = new ArrayList<>();
        for (String columnName : sortedIndexColumnNameMap.values()) {
            DataVector<Double> column = getColumn(columnName);
            columnList.add(column);
        }
        // Return the result by constructing DataFrame with list of columns
        return new DoubleDataFrame(columnList, false);
    }

    @Override
    public DataFrame<Double> select(Predicate<DataVector<Double>> rowFilter) {
        // Get the full row list
        List<DataVector<Double>> qualifiedRowList = new ArrayList<>();
        // Collect qualified row based on the filter
        for (DataVector<Double> row : getRows()) {
            if (rowFilter.test(row)) {
                qualifiedRowList.add(row);
            }
        }
        // Return the result by constructing DataFrame with list of columns
        return new DoubleDataFrame(qualifiedRowList, true);
    }

    @Override
    public DataFrame<Double> computeColumn(String columnName, Function<DataVector<Double>, Double> function) {
        // Get the original rows
        List<DataVector<Double>> originalRows = getRows();
        // Create a list to store the new values
        List<Double> newValueList = new ArrayList<>();
        // Iterate each row and apply the function to get the new value
        for (DataVector<Double> row : originalRows) {
            Double computedValue = function.apply(row);
            newValueList.add(computedValue);
        }
        // Check the new value list size
        if (newValueList.size() != getRowCount()) {
            String msg = "Something wrong! New value list size not equal to number of rows";
            throw new RuntimeException(msg);
        }
        // Expand the DataFrame with one additional column
        // The values of the new added column will be default (0.0)
        DataFrame<Double> result = expand(0, columnName);
        // Iterate through each row and set the new value to the new column
        for (int i = 0; i < getRowCount(); i++) {
            Double newValue = newValueList.get(i);
            result.setValue(i, columnName, newValue);
        }
        return result;
    }

    @Override
    public DataVector<Double> summarize(String name, BinaryOperator<Double> summaryFunction) {
        // Create empty list of data
        List<Double> summaryResultList = new ArrayList<>();
        // Iterate each column
        for (DataVector<Double> column : getColumns()) {
            List<Double> valueList = column.getValues();

            // If there's only 1 element in value list
            if (valueList.size() == 1) {
                // Then this only value is the summary value
                summaryResultList.add(valueList.get(0));
                // Continue to the next column
                continue;
            }

            // If the number of elements in value list > 1
            // First calculate the summary value with the 1st and 2nd element in value list
            Double summaryValue = summaryFunction.apply(valueList.get(0), valueList.get(1));
            // Then iterate from the 3rd element to calculate
            for (int i = 2; i < valueList.size(); i++) {
                summaryValue = summaryFunction.apply(summaryValue, valueList.get(i));
            }
            // When done calculating, add the summary value to the summary result list
            summaryResultList.add(summaryValue);
        }
        // Construct the DataVector and return the result
        return new DoubleDataVector(name, getColumnNames(), summaryResultList);
    }

    public List<List<Double>> getData() {
        return this.data;
    }

    public Map<String, Integer> getColumnNamesMap() {
        return this.columnNamesMap;
    }
}
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

    private double DEFAULT_DATA = 0.0;

    /*
    * Constructor: initialize data
    */
    public DoubleDataFrame(List<String> columnNames, double [][] data) {
        // Map the column names with the indexes
        this.columnNamesMap = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            // Iterate each column name
            String key = columnNames.get(i);
            Integer value = i;
            // and connect column name with the index
            columnNamesMap.put(key, value);
        }

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
        // Copy data
        this.data = new ArrayList<>();
        for (List<Double> rowData : other.getData()) {
            this.data.add(new ArrayList<>(rowData));
        }
        // Copy column name map
        this.columnNamesMap = new HashMap<>();
        this.columnNamesMap.putAll(other.getColumnNamesMap());
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
            throw new IllegalArgumentException("Column name not exists!");
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

        // Extend new column data
        for (List<Double> rowData : result.getData()) {
            // Iterate each new column name
            for (String newColName : newCols) {
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
                // Extend the column data with default value 0.0
                rowData.add(DEFAULT_DATA);
            }
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
        return null;
    }

    @Override
    public DataFrame<Double> select(Predicate<DataVector<Double>> rowFilter) {
        return null;
    }

    @Override
    public DataFrame<Double> computeColumn(String columnName, Function<DataVector<Double>, Double> function) {
        return null;
    }

    @Override
    public DataVector<Double> summarize(String name, BinaryOperator<Double> summaryFunction) {
        return null;
    }

    public List<List<Double>> getData() {
        return this.data;
    }

    public Map<String, Integer> getColumnNamesMap() {
        return this.columnNamesMap;
    }
}
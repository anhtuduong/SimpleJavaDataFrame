import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

public class DoubleDataFrame implements DataFrame<Double>
{
    private Map<String, Integer> columnNamesMap;
    private List<List<Double>> data;

    /*
    * Constructor: initialize data
    */
    public DoubleDataFrame(List<String> columnNames, double [][] data) {
        // Map the column names with the indexes
        this.columnNamesMap = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String key = columnNames.get(i);
            Integer value = i;
            columnNamesMap.put(key, value);
        }
        // Convert data array to lists
        this.data = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            List<Double> rowData = new ArrayList<>();
            for (int j = 0; j < data[i].length; j++) {
                rowData.add(data[i][j]);
            }
            this.data.add(rowData);
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
        return null;
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
}
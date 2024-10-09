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
        if (isValidIndexes(rowIndex, colName)) {
            // Set the value
            int columnIndex = this.columnNamesMap.get(colName);
            this.data.get(rowIndex).set(columnIndex, value);
        }
    }

    @Override
    public Double getValue(int rowIndex, String colName) throws IndexOutOfBoundsException, IllegalArgumentException {
        if (isValidIndexes(rowIndex, colName)) {
            // Set the value
            int columnIndex = this.columnNamesMap.get(colName);
            return this.data.get(rowIndex).get(columnIndex);
        }
        return null;
    }

    private boolean isValidIndexes(int rowIndex, String colName) throws IndexOutOfBoundsException, IllegalArgumentException {
        // Check row index
        if (rowIndex < 0 || rowIndex > getRowCount()) {
            throw new IndexOutOfBoundsException("Invalid row index!");
        }
        // Check column name
        if (!this.columnNamesMap.containsKey(colName)) {
            throw new IllegalArgumentException("Column name not exists!");
        }
        return true;
    }

    @Override
    public DataVector<Double> getRow(int rowIndex) throws IndexOutOfBoundsException {
        return null;
    }

    @Override
    public DataVector<Double> getColumn(String colName) throws IllegalArgumentException {
        return null;
    }

    @Override
    public List<DataVector<Double>> getRows() {
        return List.of();
    }

    @Override
    public List<DataVector<Double>> getColumns() {
        return List.of();
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

    @Override
    public DataFrameStatistics statistics() throws UnsupportedOperationException {
        return DataFrame.super.statistics();
    }

    @Override
    public DataFramePlotting plotting() throws UnsupportedOperationException {
        return DataFrame.super.plotting();
    }

    @Override
    public DataFrame<Double> expand(int additionalRows, String... newCols) throws IllegalArgumentException {
        return DataFrame.super.expand(additionalRows, newCols);
    }

    @Override
    public DataFrame<Double> project(String... retainCols) throws IllegalArgumentException {
        return DataFrame.super.project(retainCols);
    }

    @Override
    public DataFrame<Double> expandRows(int additionalRows) {
        return DataFrame.super.expandRows(additionalRows);
    }

    @Override
    public DataFrame<Double> expandColumns(List<String> newCols) {
        return DataFrame.super.expandColumns(newCols);
    }

    @Override
    public DataFrame<Double> concat(DataFrame<Double> other) throws IllegalArgumentException {
        return DataFrame.super.concat(other);
    }

    @Override
    public void print() {
        DataFrame.super.print();
    }

    @Override
    public String formatMatrix(int colWidth) {
        return DataFrame.super.formatMatrix(colWidth);
    }

    @Override
    public Iterator<DataVector<Double>> iterator() {
        return DataFrame.super.iterator();
    }
}
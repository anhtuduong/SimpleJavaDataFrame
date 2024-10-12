import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DoubleDataVector implements DataVector<Double> {

    private String name;
    private Map<String, Double> dataMap;

    public DoubleDataVector(String name, List<String> entryNames, List<Double> data) {
        this.name = name;
        // Associate entry name with its value and put into a map
        this.dataMap = new HashMap<>();
        for (int i = 0; i < entryNames.size(); i++) {
            this.dataMap.put(entryNames.get(i), data.get(i));
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public List<String> getEntryNames() {
        return new ArrayList<>(this.dataMap.keySet());
    }

    @Override
    public Double getValue(String entryName) {
        return this.dataMap.get(entryName);
    }

    @Override
    public List<Double> getValues() {
        return new ArrayList<>(this.dataMap.values());
    }

    @Override
    public Map<String, Double> asMap() {
        return this.dataMap;
    }
}

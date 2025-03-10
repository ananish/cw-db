package edu.uob;

import java.util.*;

public class Row {
    private final int id;
    private final List<String> values;

    public Row(int id, List<String> values) {
        this.id = id;
        this.values = new ArrayList<>(values);
    }

    public Row(String[] data) {
        this.id = Integer.parseInt(data[0]);
        this.values = new ArrayList<>(Arrays.asList(data).subList(1, data.length));
    }

    public int getId() {
        return id;
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return id + "\t" + String.join("\t", values);
    }
}

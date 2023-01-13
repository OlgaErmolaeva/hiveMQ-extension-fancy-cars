package org.extension.transformer;

public enum DegreeUnit {
    CELSIUS("celsius"),
    FAHRENHEIT("fahrenheit");

    private final String name;

    DegreeUnit(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}

package com.example.demo;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "demo")
public class DemoEntity {

    @Id
    @GeneratedValue
    private long id;
    private String uniqueField;

    public DemoEntity() {
    }

    public DemoEntity(String uniqueField) {
        this.uniqueField = uniqueField;
    }

    public String getUniqueField() {
        return uniqueField;
    }

    public void setUniqueField(String uniqueField) {
        this.uniqueField = uniqueField;
    }
}

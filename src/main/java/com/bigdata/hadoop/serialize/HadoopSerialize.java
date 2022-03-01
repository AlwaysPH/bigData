package com.bigdata.hadoop.serialize;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * hadoop序列化和反序列化
 * 反序列化的顺序和序列化的顺序完全一致
 */
public class HadoopSerialize {
    public static void main(String[] args) {
        StudentWriteable studentWriteable = new StudentWriteable();
        studentWriteable.setId(1L);
        studentWriteable.setName("hadoop");
    }
}

class StudentWriteable implements Writable{

    private Long id;

    private String name;

    public StudentWriteable() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();
    }
}

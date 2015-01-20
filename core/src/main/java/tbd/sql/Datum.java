package tdb.sql;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import net.sf.jsqlparser.schema.Column;

public interface Datum extends Serializable{

  public String toString();

  public String toComString();

  public Column getColumn();

  public void setColumn(Column column);

  public boolean equals(Column col);

  public int compareTo(Datum d);
  public Datum sumDatum(Datum input);

  public String getStringValue();

  public class dLong implements Datum {

    private static final long serialVersionUID = 1L;
    Long value;
    Column column;

    public dLong() {}

    public Column getColumn() {
      return column;
    }

    public void setColumn(Column column) {
      this.column = column;
    }

    public dLong(String s, Column col) {
      value = Long.parseLong(s);
      this.column = col;
    }

    public dLong(Long s, Column col) {
      value = s;
      this.column = col;
    }

    public long getValue() {
      return value;
    }

    public void setValue(long value) {
      this.value = value;
    }

    public String toString() {
      return String.valueOf(value);
    }

    public String toComString() {
      return column.getTable().getName() + ":" + column.getTable().getAlias() +
        ":" + column.getWholeColumnName() + ":" + column.getTable().getAlias() +
        ":" + String.valueOf(value) + "\t";
    }
    public int hashCode() {
       long hash = 7;
            hash = (31 * hash) + value;
            hash = (31 * hash) + (null == value ? 0 : value.hashCode());
            return (int) hash;
      }

    public boolean equals(Object obj)
        {
            if(this == obj)
                return true;
            if((obj == null) || (obj.getClass() != this.getClass()))
                return false;
            // object must be Test at this point
            dLong test = (dLong) obj;
           return (this.value == test.value)? true: false;
       }
    @Override
    public boolean equals(Column col) {
      if (col == null)
        return false;
      else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
        return false;
      else
        return true;
    }
    @Override
    public int compareTo(Datum d) {
      return this.value.compareTo(((dLong)d).value);
    }
    @Override
    public Datum sumDatum(Datum input){
      Datum sum = null;
      if(input instanceof dLong){
        long arg1 =(long) this.getValue();
        long arg2 =(long)  ((dLong) input).getValue();
        Long value = arg1+arg2;
        sum = new dLong(value,input.getColumn());
      }
      else if (input instanceof dString){
      }
      else if (input instanceof dDate){
      }
      return sum;
    }

    @Override
    public String getStringValue() {
      return value.toString();
    }

  }

  public class dDecimal implements Datum {

    private static final long serialVersionUID = 1L;

    Double value;
    Column column;

    public dDecimal(){}
    public Column getColumn() {
      return column;
    }

    public void setColumn(Column column) {
      this.column = column;
    }

    public dDecimal(String s, Column col) {
      value = Double.parseDouble(s);
      this.column = col;
    }

    public dDecimal(Double s, Column col) {
      value = s;
      this.column = col;
    }

    public dDecimal(Integer s, Column col) {
      value = Double.parseDouble(s.toString());
      this.column = col;
    }

    public Double getValue() {
      return value;
    }

    public void setValue(Double value) {
      this.value = value;
    }

    public String toString() {
      return String.format("%.2f", value);
    }

    public String toComString() {
      return column.getTable().getName() + ":" + column.getTable().getAlias() +
        ":" + column.getWholeColumnName() + ":" + column.getTable().getAlias() +
        ":" + String.valueOf(value) + "\t";
    }

    @Override
    public boolean equals(Column col) {
      if (col == null)
        return false;
      else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
        return false;
      else
        return true;
    }

    @Override
    public int compareTo(Datum d) {
      return this.value.compareTo(((dDecimal)d).value);
    }

    @Override
    public Datum sumDatum(Datum input){
      Datum sum = null;
      if(input instanceof dDecimal){
        Double value = this.getValue()+((dDecimal) input).getValue();
        sum = new dDecimal(value,input.getColumn());
      }
      else if (input instanceof dString){

      }
      else if (input instanceof dDate){

      }
    return sum;
    }

    @Override
    public String getStringValue() {
      return value.toString();
    }

  }

  public class dString implements Datum {

    private static final long serialVersionUID = 1L;
    String value;
    Column column;

    public dString(){}
    public dString(String s, Column col) {
      value = s;
      column = col;
    }

    public Column getColumn() {
      return column;
    }

    public void setColumn(Column column) {
      this.column = column;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public String toString() {
      return value;
    }

    public String toComString() {
      return column.getTable().getName() + ":" + column.getTable().getAlias() +
        ":" + column.getWholeColumnName() + ":" + value + "\t";
    }

    @Override
    public boolean equals(Column col) {
      if (col == null)
        return false;
      else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
        return false;
      else
        return true;
    }
    public int compareTo(Datum d) {
      return this.value.compareTo(((dString)d).value);
    }

    public int hashCode() {
       String hash = "hash";
       int ret;
            hash = hash.concat(value);
            ret = hash.hashCode()+ (null == value ? 0 : value.hashCode());
            return ret;
      }

    public boolean equals(Object obj)
        {
            if(this == obj)
                return true;
            if((obj == null) || (obj.getClass() != this.getClass()))
                return false;
            dString test = (dString) obj;
           return (this.value.equals(test.value))? true: false;
       }

    @Override
    public String getStringValue() {
      return value.toString();
    }

    @Override
    public Datum sumDatum(Datum input) {
      return null;
    }

  }

  public class dDate implements Datum {

    private static final long serialVersionUID = 1L;
    Date value;
    int year;
    int month;
    int day;
    Column column;

    public dDate(){}
    public dDate(String s, Column col) {

      try {
        value = (new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
            .parse(s));
        Calendar cal = Calendar.getInstance();
        cal.setTime(value);
        if (value != null) {
          year = cal.get(Calendar.YEAR);
          month = cal.get(Calendar.MONTH)+1;
          day = cal.get(Calendar.DAY_OF_MONTH);

        }
        column = col;
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    public dDate(Date s, Column col) {
      value = s;
      this.column = col;
    }

    public Date getValue() {
      return value;
    }

    public void setValue(Date value) {
      this.value = value;
    }

    public String toString() {
      return String.format("%04d-%02d-%02d", year, month, day);
    }

    public String toComString() {
      return column.getTable().getName() + ":" + column.getTable().getAlias() +
        ":" + column.getWholeColumnName() + ":" +
        String.format("%04d-%02d-%02d", year, month, day) + "\t";
    }

    @Override
    public Column getColumn() {
      return column;
    }

    @Override
    public void setColumn(Column column) {
      this.column = column;

    }

    @Override
    public boolean equals(Column col) {
      if (col == null)
        return false;
      else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
        return false;
      else
        return true;
    }

    public int compareTo(Datum d) {
      return this.value.compareTo(((dDate)d).value);
    }

    @Override
    public String getStringValue() {
      return null;
    }

    @Override
    public Datum sumDatum(Datum input) {
      return null;
    }
  }
}

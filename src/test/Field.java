/**
 * Created by bgk on 6/7/17.
 */
public class Field {
    private String fieldname;
    private boolean isPrimaryKey;
    private Object beforeUpdateValue;
    private Object afterUpdateValue;

    public Field(String fieldname, boolean isPrimaryKey, Object beforeUpdateValue, Object afterUpdateValue) {
        this.fieldname = fieldname;
        this.isPrimaryKey = isPrimaryKey;
        this.beforeUpdateValue = beforeUpdateValue;
        this.afterUpdateValue = afterUpdateValue;
    }

    public void setFieldname(String fieldname) {
        this.fieldname = fieldname;
    }

    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public void setBeforeUpdateValue(String beforeUpdateValue) {
        this.beforeUpdateValue = beforeUpdateValue;
    }

    public void setBeforeUpdateValue(long beforeUpdateValue) {
        this.beforeUpdateValue = beforeUpdateValue;
    }

    public void setAfterUpdateValue(String afterUpdateValue) {
        this.afterUpdateValue = afterUpdateValue;
    }

    public void setAfterUpdateValue(long afterUpdateValue) {
        this.afterUpdateValue = afterUpdateValue;
    }

    public String getFieldname() {
        return fieldname;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
}

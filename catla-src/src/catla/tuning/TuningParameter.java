package cn.edu.bjtu.cdh.catla.tuning;

public class TuningParameter {
	
	public static String RANGE="range";

	
	public static String ARRAY="array";
	
	public static String FIXED_VALUE="fixedvalue";
	
	public static String INT="int";
	public static String FLOAT="float";
	public static String STRING="string";
	
	private String[] alias;
	
	private String target;
	
	private String numberType;
	
	private String name;
	private String range;
	private String type;
	private double min;
	private double max;
	private double step;
	private String[] valueSet;
	private String value;
	public String getName() {
		return name;
	}
	
	private String defaultValue;
	
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getRange() {
		return range;
	}
	public void setRange(String range) {
		this.range = range;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public double getMin() {
		return min;
	}
	public void setMin(double min) {
		this.min = min;
	}
	public double getMax() {
		return max;
	}
	public void setMax(double max) {
		this.max = max;
	}
	public double getStep() {
		return step;
	}
	public void setStep(double step) {
		this.step = step;
	}
	public String[] getValueSet() {
		return valueSet;
	}
	public void setValueSet(String[] valueSet) {
		this.valueSet = valueSet;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getNumberType() {
		return numberType;
	}
	public void setNumberType(String numberType) {
		this.numberType = numberType;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public String[] getAlias() {
		return alias;
	}
	public void setAlias(String[] alias) {
		this.alias = alias;
	}
}

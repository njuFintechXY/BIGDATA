package relationpackage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


//关系A,为了使其selection操作能匹配不等号，此处在原代码的基础上重写了isCondition()函数
public class RelationA implements WritableComparable<RelationA>{
	private int id;
	private String name;
	private int age;
	private double weight;
	
	public RelationA(){}
	
	public RelationA(int id, String name, int age, double weight){
		this.setId(id);
		this.setName(name);
		this.setAge(age);
		this.setWeight(weight);
	}
	
	public RelationA(String line){
		String[] value = line.split(",");
		this.setId(Integer.parseInt(value[0]));
		this.setName(value[1]);
		this.setAge(Integer.parseInt(value[2]));
		this.setWeight(Double.parseDouble(value[3]));
	}
	
	protected boolean iscondi(int value1,int value2,String symbol) {
		//简化下一个函数代码
		if(symbol.equals("=") && value1 == value2)
			return true;
		else if(symbol.equals(">") && value1 > value2)
			return true;
		else if(symbol.equals("<") && value1 < value2)
			return true;
		else if(symbol.equals(">=") && value1 >= value2)
			return true;
		else if(symbol.equals("<=") && value1 <= value2)
			return true;
		else if(symbol.equals("!=") && value1 != value2)
			return true;
		else
			return false;
	}
	public boolean isCondition(String col,String symbol,String value){
		/*
		if(symbol.equals("=")) {
			if(col == 0 && Integer.parseInt(value) == this.id)
				return true;
			else if(col == 1 && name.equals(value))
				return true;
			else if(col ==2 && Integer.parseInt(value) == this.age)
				return true;
			else if(col ==3 && Double.parseDouble(value) == this.weight)
				return true;
			else
				return false;
		}
		else if(symbol.equals(">=")) {
			
		}*/
		//col为列名
		switch(col) {
		case "id": return this.iscondi(this.id, Integer.parseInt(value), symbol);
		case "age": return this.iscondi(this.age, Integer.parseInt(value), symbol);
		case "weight": return this.iscondi((int)this.weight, Integer.parseInt(value), symbol);
		case "name": 
			if(symbol.equals("=") && this.name.equals(value))
				return true;
			else if(symbol.equals("!=") && !this.name.equals(value))
				return true;
			else
				return false;
		default:return false;
		}
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	public String getCol(String col){
		switch(col){
		case "id": return String.valueOf(id);
		case "name": return name;
		case "age": return String.valueOf(age); 
		case "weight": return String.valueOf(weight);
		default: return null;
		}
	}
	
	public String getValueExcept(int col){
		switch(col){
		case 0: return name + "," + String.valueOf(age) + "," + String.valueOf(weight);
		case 1: return String.valueOf(id) + "," + String.valueOf(age) + "," + String.valueOf(weight);
		case 2: return String.valueOf(id) + "," + name + "," + String.valueOf(weight);
		case 3: return String.valueOf(id) + "," + name + "," + String.valueOf(age);
		default: return null;
		}
	}
	
	@Override
	public String toString(){
		return id + "," + name + "," + age + "," + weight;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
		out.writeDouble(weight);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readInt();
		name = in.readUTF();
		age = in.readInt();
		weight = in.readDouble();
	}

	@Override
	public int compareTo(RelationA o) {
		if(id == o.getId() && name.equals(o.getName()) 
				&& age == o.getAge() && weight == o.getWeight())
			return 0;
		else if(id < o.getId())
			return -1;
		else
			return 1;
	}
	
}
package pojo;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

// 根据格式化后的SQL构造
public class POJOBuilder {

    public static void main(String[] args) throws IOException {
        // 读取ch.sql
        File sql = new File(ClassLoader.getSystemResource("ch.sql").getPath());
        if (sql.exists()) {
            BufferedReader in = new BufferedReader(new FileReader(sql));
            File pojo;
            String line;
            BufferedWriter out = null;
            String className = null;
            String fullName = null;
            // 维护POJO的属性和类型之间的对应关系
            Map<String, Class<?>> properties = null;
            while ((line = in.readLine()) != null) {
                line = line.trim().toLowerCase();
                // 读到建表语句，说明应当新写一个POJO类，此时记录POJO的类名，并设置BufferedWriter指向
                if (line.startsWith("create table")) {
                    line = line.substring(12).trim();
                    fullName = line;
                    className = line.substring(line.lastIndexOf('.') + 1);
                    pojo = new File(System.getProperties().getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "java" + File.separator + "pojo" + File.separator + className + ".java");
                    if ((!pojo.exists() || pojo.exists() && pojo.delete()) && pojo.createNewFile()) {
                        out = new BufferedWriter(new FileWriter(pojo));
                    }
                    properties = new HashMap<>();
                } else if (line.contains("string")) {
                    // 读到建表语句中的String，说明该属性是String类型
                    String propertiesName = line.split("\\s+")[0];
                    assert properties != null;
                    properties.put(propertiesName, String.class);
                } else if (line.contains("decimal")) {
                    // 读到建表语句中的decimal，说明该属性是Double类型
                    String propertiesName = line.split("\\s+")[0];
                    assert properties != null;
                    properties.put(propertiesName, Double.class);
                } else if (line.contains("int")) {
                    // 读到建表语句中的int，说明该属性是Integer类型
                    String propertiesName = line.split("\\s+")[0];
                    assert properties != null;
                    properties.put(propertiesName, Integer.class);
                } else if (line.startsWith(")")) {
                    // 读到），说明建表语句结束，获取POJO信息完毕，此时开始将信息写入POJO类
                    StringBuilder content = new StringBuilder();
                    // 定义换行符号和双换号符号
                    String lineSeparator = System.lineSeparator(), doubleLineSeparator = lineSeparator + lineSeparator;
                    // init，POJO的包、依赖关系及类名
                    {
                        assert out != null;
                        content.append("package pojo;").append(doubleLineSeparator);
                        content.append("import lombok.Data;").append(lineSeparator);
                        content.append("import java.sql.Connection;").append(lineSeparator);
                        content.append("import java.sql.PreparedStatement;").append(lineSeparator);
                        content.append("import java.sql.SQLException;").append(doubleLineSeparator);
                        content.append("@Data").append(lineSeparator);
                        content.append("public class ").append(className).append(" implements Insert {").append(doubleLineSeparator);
                    }
                    // properties，POJO的属性，并为double和int值赋初值（避免json空值可能造成的问题）
                    {
                        for (Map.Entry<String, Class<?>> entry : properties.entrySet()) {
                            content.append("\tpublic ").append(entry.getValue().getSimpleName()).append(" ").append(entry.getKey());
                            if (entry.getValue().equals(Integer.class)) {
                                content.append(" = 0");
                            } else if (entry.getValue().equals(Double.class)) {
                                content.append(" = 0.0");
                            }
                            content.append(";").append(doubleLineSeparator);
                        }
                    }
                    // stat & insert，实现insert接口
                    {
                        content.append("\t@Override").append(lineSeparator);
                        content.append("\tpublic PreparedStatement stat(Connection con) {").append(lineSeparator);
                        content.append("\t\ttry {").append(lineSeparator);
                        content.append("\t\t\treturn con.prepareStatement(\"INSERT INTO ").append(fullName).append(" (");
                        int index = 0;
                        // 根据属性拼接PreparedStatement需要的的sql语句
                        for (String propertiesName : properties.keySet()) {
                            content.append(propertiesName);
                            if (++index != properties.size()) content.append(", ");
                            else content.append(") VALUES (");
                        }
                        while (index-- > 1) {
                            content.append("?, ");
                        }
                        content.append("?)\");").append(lineSeparator);
                        content.append("\t\t} catch (SQLException e) {").append(lineSeparator);
                        content.append("\t\t\te.printStackTrace();").append(lineSeparator);
                        content.append("\t\t}").append(lineSeparator);
                        content.append("\t\treturn null;").append(lineSeparator);
                        content.append("\t}").append(doubleLineSeparator);

                        // 根据属性为PreparedStatement赋值
                        content.append("\t@Override").append(lineSeparator);
                        content.append("\tpublic PreparedStatement insert(PreparedStatement preparedStatement) {").append(lineSeparator);
                        content.append("\t\ttry {").append(lineSeparator);
                        for (Map.Entry<String, Class<?>> entry : properties.entrySet()) {
                            if (entry.getValue().equals(Integer.class))
                                content.append("\t\t\tpreparedStatement.setInt").append('(').append(++index).append(", ").append(entry.getKey()).append(");").append(lineSeparator);
                            else
                                content.append("\t\t\tpreparedStatement.set").append(entry.getValue().getSimpleName()).append('(').append(++index).append(", ").append(entry.getKey()).append(");").append(lineSeparator);
                        }
                        content.append("\t\t\treturn preparedStatement;").append(lineSeparator);
                        content.append("\t\t} catch (SQLException e) {").append(lineSeparator);
                        content.append("\t\t\te.printStackTrace();").append(lineSeparator);
                        content.append("\t\t}").append(lineSeparator);
                        content.append("\t\treturn preparedStatement;").append(lineSeparator);
                        content.append("\t}").append(lineSeparator);
                    }
                    // finish
                    {
                        content.append("}").append(lineSeparator);
                        out.write(content.toString());
                        out.close();
                    }
                }
            }
        }
    }
}
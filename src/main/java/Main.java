import com.alibaba.fastjson.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/home/approdite/IdeaProjects/kafka/bank2.txt"));
            int bufferSize = 10 * 1024 * 1024;
            BufferedReader in = new BufferedReader(new InputStreamReader(bis), bufferSize);
            String line;
            int c = 1;
            while ((line = in.readLine()) !=  null && !line.isEmpty()) {
                c++;
                int f = 0;
                for (int i = 0; i < line.length(); i++) {
                    if (line.charAt(i) == '{') {
                        f++;
                    } else if (line.charAt(i) == '}') {
                        f--;
                    }
                }
                if (f != 0)
                    System.err.println(c + "：" + line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

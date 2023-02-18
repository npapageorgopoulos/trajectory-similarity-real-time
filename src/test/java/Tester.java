import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Stream;

public class Tester {
    public static void main(String[] args) {
        String fileName = "/Users/np12071/Downloads/lab2018_oranges_clean.csv";
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            Stream<String> lines = br.lines();
            lines.forEach(line -> {
                String value = line;
                System.out.println(value);
            });
        } catch (IOException e) {
            System.out.println("Error reading file: " + fileName);
        }
    }
}

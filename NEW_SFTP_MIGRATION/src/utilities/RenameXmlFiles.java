package utilities;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class RenameXmlFiles {
    public static void main(String[] args) {
        // Specify the directory containing the XML files
        Path dir = Paths.get("D:\\Data\\data");

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.xml")) {
            for (Path entry : stream) {
                // Get the file name and its extension
                String fileName = entry.getFileName().toString();
                
                // Create the new file name by prefixing "091"
                String newFileName = "091" + fileName;
                
                // Construct the new file path
                Path newFilePath = dir.resolve(newFileName);

                // Rename the file by moving it to the new path
                Files.move(entry, newFilePath, StandardCopyOption.REPLACE_EXISTING);
                
                System.out.println("Renamed: " + fileName + " to " + newFileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


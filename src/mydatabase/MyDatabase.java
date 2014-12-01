package mydatabase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author Sunish
 */
public class MyDatabase {

    long eachCharPosition;
    long position;
    String searchIndex;
    String searchLast_name;
    int SearchSuccess = 0;
    String indexMatchIndex;
    String delete;
    String insert;
    int count;
    String updateForIndex;
    String updateIndex;
    String updateString;

    public static void main(String[] args) throws Exception {
        MyDatabase mydb = new MyDatabase();
        mydb.Data();
        //mydb.creatingIndexIDFile(0);
        //mydb.creatingIndexLast_nameFile(0);
        //mydb.readingDatabase();
        //mydb.readIndexId();
        //mydb.SearchUsingIndexId(mydb.searchIndex, 0);
        //mydb.SearchUsingIndexLastName(mydb.searchLast_name, 0);
        mydb.insertNewRecord();
        //mydb.DeleteRecord();
        //mydb.updateRecord();
        //mydb.finalPrint();
    }

    public void Data() throws IOException {
        // Updating Data
        updateString = "Ruh";
        updateIndex = "first_name";
        updateForIndex = "Patel";
        //Search using Index as Key
        searchIndex = "2";
        //Search using Last Name as Key
        searchLast_name = "kaul";
        // Delete using any key
        delete = "nainani";
        // Insert data
        /*try {
            File file = new File("book_copies_insert.txt");
            FileWriter fileWritter = new FileWriter(file.getName(), true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            BufferedReader r = new BufferedReader(new FileReader("book_copies.csv"));
            String line = r.readLine();
            if (line != null) {
                do {
                    Object rowData[] = line.split("\t");// Split the text file rows using tab spacing.
                    String insert = "INSERT INTO Book_copies values ('" + rowData[0] + "', " + rowData[1] + "," + rowData[2] + ");" + "\n";
                    bufferWritter.write(insert);
                    System.out.print(insert);
                    line = r.readLine();
                } while (line != null);
            }
        } catch (IOException e) {
            System.out.println(e);
        }*/
        insert = "'500','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt@gmail.com','http://www.bentonjohnbjr.com'";

    }

    public void creatingIndexIDFile(int flag) throws Exception {
        File f = new File("Data.db");
        File f2 = new File("id.ndx");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
        new RandomAccessFile(f2, "rw").setLength(0);
        if (flag == 0) {
            System.out.println("##### Creating the Index Id file #####");
        }
        count = 0;
        HashMap<String, String> map = new HashMap<>();
        String eachIndex;
        eachCharPosition = 0;
        eachIndex = "" + eachCharPosition;
        position = raf.length();
        while (eachCharPosition < position) {
            count++;
            eachIndex = "" + eachCharPosition;
            raf.seek(eachCharPosition);
            String line = "";
            byte ch = raf.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf.readByte();
                eachCharPosition++;
            }
            eachCharPosition++;
            String eachAttribute[];
            eachAttribute = line.split("\t");
            map.put(eachAttribute[0], eachIndex);
        }
        String[] keys = (String[]) map.keySet().toArray(new String[0]);
        /*for(int k = 1 ; k < keys.length-1 ; k++){
         int temp = Integer.parseInt(keys[k]);
         int l;
         for(l = k-1;l>=0 && temp<Integer.parseInt(keys[l]);l--)
         keys[k+1] = keys[k];
         keys[k+1] = Integer.toString(temp);
         }*/
        int[] numbers = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            numbers[i] = Integer.parseInt(keys[i]);
        }
        Arrays.sort(numbers);
        for (int i = 0; i < keys.length; i++) {
            keys[i] = Integer.toString(numbers[i]);
        }
        for (String key : keys) {
            String write = key + "\t" + map.get(key) + "\n";
            raf2.write(write.getBytes());
            if (flag == 0) {
                System.out.println(key + "\t" + map.get(key));
            }
        }
    }

    public void creatingIndexLast_nameFile(int flag) throws Exception {
        File f = new File("Data.db");
        File f3 = new File("last_name.ndx");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        RandomAccessFile raf3 = new RandomAccessFile(f3, "rw");
        new RandomAccessFile(f3, "rw").setLength(0);
        HashMap<String, String> map = new HashMap<>();
        if (flag == 0) {
            System.out.println("##### Creating the Index Last_name file #####");
        }
        String eachIndex;
        eachCharPosition = 0;
        eachIndex = "" + eachCharPosition;
        position = raf.length();
        while (eachCharPosition < position) {
            eachIndex = "" + eachCharPosition;
            raf.seek(eachCharPosition);
            String line = "";
            byte ch = raf.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf.readByte();
                eachCharPosition++;
            }
            eachCharPosition++;
            String eachAttribute[];
            eachAttribute = line.split("\t");
            String checkHashMap = "";
            checkHashMap = map.get(eachAttribute[2]);
            if (checkHashMap == null) {
                map.put(eachAttribute[2], eachIndex);
            } else {
                checkHashMap = checkHashMap + ',' + eachIndex;
                map.put(eachAttribute[2], checkHashMap);
            }
        }
        String[] keys = (String[]) map.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        for (String key : keys) {
            String write = key + "\t" + map.get(key) + "\n";
            raf3.write(write.getBytes());
            if (flag == 0) {
                System.out.println(key + "\t" + map.get(key));
            }
        }
    }

    public void readingDatabase() throws Exception {
        File f = new File("Data.db");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        System.out.println("##### Reading the Database file #####");
        eachCharPosition = 0;
        position = raf.length();
        while (eachCharPosition < position) {
            System.out.println(eachCharPosition);
            raf.seek(eachCharPosition);
            String line = "";
            byte ch = raf.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf.readByte();
                eachCharPosition++;
            }
            eachCharPosition++;
            String eachAttribute[];
            eachAttribute = line.split("\t");
            for (int i = 0; i < eachAttribute.length; i++) {
                System.out.print(eachAttribute[i] + "\t");
            }
        }
        position = raf.length();
        raf.seek(position);
        System.out.println(position);
    }

    public void readIndexId() throws Exception {
        File f2 = new File("id.ndx");
        RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
        System.out.println("##### Reading the Index ID file #####");
        eachCharPosition = 0;
        position = raf2.length();
        count = 0;
        while (eachCharPosition < position) {
            count++;
            System.out.println(eachCharPosition);
            raf2.seek(eachCharPosition);
            String line = "";
            byte ch = raf2.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf2.readByte();
                eachCharPosition++;
            }
            eachCharPosition++;
            String eachAttribute[];
            eachAttribute = line.split("\t");
            for (int i = 0; i < eachAttribute.length; i++) {
                System.out.print(eachAttribute[i] + "\t");
            }
        }
        position = raf2.length();
        raf2.seek(position);
        System.out.println(position);
    }

    public boolean SearchUsingIndexId(String searchIndex, int flag) throws Exception {
        File f = new File("Data.db");
        File f2 = new File("id.ndx");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
        if (flag == 0) {
            System.out.println("##### Searching from Database File using Id index file #####");
        }
        SearchSuccess = 0;
        indexMatchIndex = "0";
        eachCharPosition = 0;
        position = raf2.length();
        while (eachCharPosition < position) {
            raf2.seek(eachCharPosition);
            String line = "";
            byte ch = raf2.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf2.readByte();
                eachCharPosition++;
            }
            eachCharPosition++;
            String eachAttribute[];
            eachAttribute = line.split("\t");
            if (eachAttribute[0].equals(searchIndex)) {
                indexMatchIndex = eachAttribute[1];
                SearchSuccess = 1;
            }
        }
        if (SearchSuccess == 1) {
            raf.seek(Long.parseLong(indexMatchIndex));
            String line = "";
            byte ch = raf.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf.readByte();
            }
            String eachAttribute[];
            eachAttribute = line.split("\t");
            for (int i = 0; i < eachAttribute.length; i++) {
                if (flag == 0) {
                    System.out.print(eachAttribute[i] + "\t");
                }
            }
            if (flag == 0) {
                System.out.println();
            }
            return true;
        } else {
            if (flag == 0) {
                System.out.println("Match Not found");
            }
            return false;
        }

    }

    public boolean SearchUsingIndexLastName(String searchIndex, int flag) throws Exception {
        File f = new File("Data.db");
        File f3 = new File("last_name.ndx");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        RandomAccessFile raf3 = new RandomAccessFile(f3, "rw");
        if (flag == 0) {
            System.out.println("##### Searching from Database File using Last_name index file #####");
        }
        SearchSuccess = 0;
        indexMatchIndex = "0";
        eachCharPosition = 0;
        position = raf3.length();
        while (eachCharPosition < position) {
            raf3.seek(eachCharPosition);
            String line = "";
            byte ch = raf3.readByte();
            while ((char) ch != '\n') {
                line = line + (char) ch;
                ch = raf3.readByte();
                eachCharPosition++;
            }
            eachCharPosition++;
            String eachAttribute[];
            eachAttribute = line.split("\t");
            if (eachAttribute[0].equalsIgnoreCase(searchIndex)) {
                indexMatchIndex = eachAttribute[1];
                SearchSuccess = 1;
            }
        }
        if (SearchSuccess == 1) {
            String eachIndex[] = indexMatchIndex.split(",");
            for (int i = 0; i < eachIndex.length; i++) {
                raf.seek(Long.parseLong(eachIndex[i]));
                String line = "";
                byte ch = raf.readByte();
                while ((char) ch != '\n') {
                    line = line + (char) ch;
                    ch = raf.readByte();
                }
                String eachAttribute[];
                eachAttribute = line.split("\t");
                for (int j = 0; j < eachAttribute.length; j++) {
                    if (flag == 0) {
                        System.out.print(eachAttribute[j] + "\t");
                    }
                }
                if (flag == 0) {
                    System.out.println();
                }
                return true;
            }
        } else {
            if (flag == 0) {
                System.out.println("Match Not found");
            }
            return false;
        }
        return false;
    }

    public void insertNewRecord() throws Exception {
        File f = new File("Data.db");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        String insertEachRecord[] = insert.split("',");
        for (int i = 0; i < insertEachRecord.length; i++) {
            if (insertEachRecord[i].contains("'")) {
                insertEachRecord[i] = insertEachRecord[i].replace("'", "");
            }
        }
        if (SearchUsingIndexId(insertEachRecord[0], 1)) {
            System.out.println("Insert Unsuccessful");
        } else if (insertEachRecord.length < 13 || insertEachRecord.length > 13) {
            System.out.println("Insert Unsuccessful, Please Enter all the fields");
        } else {
            insert = "";
            for (int i = 0; i < insertEachRecord.length; i++) {
                insert = insert + insertEachRecord[i] + "\t";
            }
            //int index123 = Integer.parseInt(insertEachRecord[0]);
            insert = insert.trim();
            insert = insert + "\n";
            //System.out.print(index123 + "\t");
            System.out.print(insert);
            position = raf.length();
            raf.seek(position);
            //raf.write(index123);
            //position = raf.length();
            //raf.seek(position);
            //raf.write("\t".getBytes());
            //position = raf.length();
            //raf.seek(position);
            raf.write(insert.getBytes());
            creatingIndexIDFile(1);
            creatingIndexLast_nameFile(1);
        }
    }

    public void DeleteRecord() throws Exception {
        File f = new File("Data.db");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int flag = 0;
        if (SearchUsingIndexLastName(delete, 1)) {
            flag = 1;
            HashMap<String, String> map = new HashMap<>();
            String writeInFile = "";
            File f3 = new File("last_name.ndx");
            RandomAccessFile raf3 = new RandomAccessFile(f3, "rw");
            System.out.println("##### Deleting from Database File using Last_name index file #####");
            SearchSuccess = 0;
            indexMatchIndex = "0";
            eachCharPosition = 0;
            position = raf3.length();
            while (eachCharPosition < position) {
                raf3.seek(eachCharPosition);
                String line = "";
                byte ch = raf3.readByte();
                while ((char) ch != '\n') {
                    line = line + (char) ch;
                    ch = raf3.readByte();
                    eachCharPosition++;
                }
                eachCharPosition++;
                String eachAttribute[];
                eachAttribute = line.split("\t");
                if (!eachAttribute[0].equalsIgnoreCase(delete)) {
                    indexMatchIndex = eachAttribute[1];
                    String eachIndex[] = indexMatchIndex.split(",");
                    for (int i = 0; i < eachIndex.length; i++) {
                        writeInFile = "";
                        raf.seek(Long.parseLong(eachIndex[i]));
                        line = "";
                        ch = raf.readByte();
                        while ((char) ch != '\n') {
                            line = line + (char) ch;
                            ch = raf.readByte();
                        }
                        eachAttribute = line.split("\t");
                        for (int j = 1; j < eachAttribute.length; j++) {
                            writeInFile = writeInFile + eachAttribute[j] + "\t";
                        }
                        writeInFile = writeInFile.trim();
                        map.put(eachAttribute[0], writeInFile);
                        writeInFile = writeInFile + "\n";
                    }
                }
            }
            new RandomAccessFile(f, "rw").setLength(0);
            raf.seek(0);
            String[] keys = (String[]) map.keySet().toArray(new String[0]);
            Arrays.sort(keys);
            for (String key : keys) {
                String write = key + "\t" + map.get(key) + "\n";
                System.out.print(write);
                raf.write(write.getBytes());
            }
            creatingIndexIDFile(1);
            creatingIndexLast_nameFile(1);
        }

        if (SearchUsingIndexId(delete, 1)) {
            flag = 1;
            String writeInFile = "";
            File f2 = new File("id.ndx");
            RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
            System.out.println("##### Deleting from Database File using Id index file #####");
            SearchSuccess = 0;
            //searchIndex = "1";
            indexMatchIndex = "0";
            eachCharPosition = 0;
            position = raf2.length();
            while (eachCharPosition < position) {
                raf2.seek(eachCharPosition);
                String line = "";
                byte ch = raf2.readByte();
                while ((char) ch != '\n') {
                    line = line + (char) ch;
                    ch = raf2.readByte();
                    eachCharPosition++;
                }
                eachCharPosition++;
                String eachAttribute[];
                eachAttribute = line.split("\t");
                if (!eachAttribute[0].equalsIgnoreCase(delete)) {
                    indexMatchIndex = eachAttribute[1];
                    raf.seek(Long.parseLong(indexMatchIndex));
                    line = "";
                    ch = raf.readByte();
                    while ((char) ch != '\n') {
                        line = line + (char) ch;
                        ch = raf.readByte();
                    }
                    eachAttribute = line.split("\t");
                    for (int i = 0; i < eachAttribute.length; i++) {
                        writeInFile = writeInFile + eachAttribute[i] + "\t";
                    }
                    writeInFile = writeInFile.trim();
                    writeInFile = writeInFile + "\n";
                }
            }
            System.out.print(writeInFile);
            new RandomAccessFile(f, "rw").setLength(0);
            raf.seek(0);
            raf.write(writeInFile.getBytes());
            creatingIndexIDFile(1);
            creatingIndexLast_nameFile(1);
        }
        if (flag == 1) {
            System.out.println("Record deleted");
        } else {
            System.out.println("Record cannot be deleted");
        }
    }

    public void updateRecord() throws Exception {
        File f = new File("Data.db");
        String writeInFile = "";
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int updateIndexNumber = -1;
        int flag = 0;
        switch (updateIndex) {
            case "first_name":
                updateIndexNumber = 1;
                break;
            case "last_name":
                updateIndexNumber = 2;
                break;
            case "company_name":
                updateIndexNumber = 3;
                break;
            case "address":
                updateIndexNumber = 4;
                break;
            case "city":
                updateIndexNumber = 5;
                break;
            case "county":
                updateIndexNumber = 6;
                break;
            case "state":
                updateIndexNumber = 7;
                break;
            case "zip":
                updateIndexNumber = 8;
                break;
            case "phone1":
                updateIndexNumber = 9;
                break;
            case "phone2":
                updateIndexNumber = 10;
                break;
            case "email":
                updateIndexNumber = 11;
                break;
            case "web":
                updateIndexNumber = 12;
                break;
            default:
                System.out.println("Update is not possible as you are either trying to chnage the primary key or changing attribute which does not exist");
                updateIndexNumber = -1;
        }
        if (SearchUsingIndexLastName(updateForIndex, 1)) {
            flag = 1;
            File f3 = new File("last_name.ndx");
            HashMap<String, String> map = new HashMap<>();
            RandomAccessFile raf3 = new RandomAccessFile(f3, "rw");
            System.out.println("##### Updating from Database File using Last Name index file #####");
            SearchSuccess = 0;
            //searchIndex = "1";
            indexMatchIndex = "0";
            eachCharPosition = 0;
            position = raf3.length();
            while (eachCharPosition < position) {
                raf3.seek(eachCharPosition);
                String line = "";
                byte ch = raf3.readByte();
                while ((char) ch != '\n') {
                    line = line + (char) ch;
                    ch = raf3.readByte();
                    eachCharPosition++;
                }
                eachCharPosition++;
                String eachAttribute[];
                eachAttribute = line.split("\t");
                if (eachAttribute[0].equalsIgnoreCase(updateForIndex)) {
                    indexMatchIndex = eachAttribute[1];
                    String eachIndex[] = indexMatchIndex.split(",");
                    for (int j = 0; j < eachIndex.length; j++) {
                        writeInFile = "";
                        raf.seek(Long.parseLong(eachIndex[j]));
                        line = "";
                        ch = raf.readByte();
                        while ((char) ch != '\n') {
                            line = line + (char) ch;
                            ch = raf.readByte();
                        }
                        eachAttribute = line.split("\t");
                        if (updateIndexNumber > 0) {
                            eachAttribute[updateIndexNumber] = updateString;
                        }
                        for (int i = 1; i < eachAttribute.length; i++) {
                            writeInFile = writeInFile + eachAttribute[i] + "\t";
                        }
                        writeInFile = writeInFile.trim();
                        map.put(eachAttribute[0], writeInFile);
                        writeInFile = writeInFile + "\n";
                    }
                } else {
                    indexMatchIndex = eachAttribute[1];
                    String eachIndex[] = indexMatchIndex.split(",");
                    for (int j = 0; j < eachIndex.length; j++) {
                        writeInFile = "";
                        raf.seek(Long.parseLong(eachIndex[j]));
                        line = "";
                        ch = raf.readByte();
                        while ((char) ch != '\n') {
                            line = line + (char) ch;
                            ch = raf.readByte();
                        }
                        eachAttribute = line.split("\t");
                        for (int i = 1; i < eachAttribute.length; i++) {
                            writeInFile = writeInFile + eachAttribute[i] + "\t";
                        }
                        writeInFile = writeInFile.trim();
                        map.put(eachAttribute[0], writeInFile);
                    }
                }
            }
            new RandomAccessFile(f, "rw").setLength(0);
            raf.seek(0);
            String[] keys = (String[]) map.keySet().toArray(new String[0]);
            Arrays.sort(keys);
            for (String key : keys) {
                String write = key + "\t" + map.get(key) + "\n";
                System.out.print(write);
                raf.write(write.getBytes());
            }
            creatingIndexIDFile(1);
            creatingIndexLast_nameFile(1);
        }
        if (SearchUsingIndexId(updateForIndex, 1)) {
            flag = 1;
            File f2 = new File("id.ndx");
            RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
            System.out.println("##### Updating from Database File using Id index file #####");
            SearchSuccess = 0;
            //searchIndex = "1";
            indexMatchIndex = "0";
            eachCharPosition = 0;
            position = raf2.length();
            while (eachCharPosition < position) {
                raf2.seek(eachCharPosition);
                String line = "";
                byte ch = raf2.readByte();
                while ((char) ch != '\n') {
                    line = line + (char) ch;
                    ch = raf2.readByte();
                    eachCharPosition++;
                }
                eachCharPosition++;
                String eachAttribute[];
                eachAttribute = line.split("\t");
                if (eachAttribute[0].equalsIgnoreCase(updateForIndex)) {
                    indexMatchIndex = eachAttribute[1];
                    raf.seek(Long.parseLong(indexMatchIndex));
                    line = "";
                    ch = raf.readByte();
                    while ((char) ch != '\n') {
                        line = line + (char) ch;
                        ch = raf.readByte();
                    }
                    eachAttribute = line.split("\t");
                    if (updateIndexNumber > 0) {
                        eachAttribute[updateIndexNumber] = updateString;
                    }
                    for (int i = 0; i < eachAttribute.length; i++) {
                        writeInFile = writeInFile + eachAttribute[i] + "\t";
                    }
                    writeInFile = writeInFile + "\n";
                } else {
                    indexMatchIndex = eachAttribute[1];
                    raf.seek(Long.parseLong(indexMatchIndex));
                    line = "";
                    ch = raf.readByte();
                    while ((char) ch != '\n') {
                        line = line + (char) ch;
                        ch = raf.readByte();
                    }
                    eachAttribute = line.split("\t");
                    for (int i = 0; i < eachAttribute.length; i++) {
                        writeInFile = writeInFile + eachAttribute[i] + "\t";
                    }
                    writeInFile = writeInFile.trim();
                    writeInFile = writeInFile + "\n";
                }
            }
            System.out.print(writeInFile);
            new RandomAccessFile(f, "rw").setLength(0);
            raf.seek(0);
            raf.write(writeInFile.getBytes());
            creatingIndexIDFile(1);
            creatingIndexLast_nameFile(1);
        }
        if (updateIndexNumber == -1) {
            System.out.println("Update is not possible as you are either trying to chnage the primary key or changing attribute which does not exist");
        }
        if ((flag == 1) && (updateIndexNumber != -1)) {
            System.out.println("Updated a record");
        } else if (flag == 0) {
            System.out.println("Update not possible as Index not found");
        }
    }

    public void finalPrint() {
        System.out.println("##### Total number of records #####");
        System.out.println(count);
    }
}

package ru.fizteh.fivt.students.baranov.wordcounter;

import ru.fizteh.fivt.file.WordCounter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MyWordCounter implements WordCounter {
    public String ls = System.lineSeparator();

    public void count(List<File> files, OutputStream out, boolean aggregate) throws IOException {
        Map<String, Integer> mapOfWords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("files for counting not found");
        }


        for (int i = 0; i < files.size(); ++i) {
            if (!files.get(i).exists()) {
                if (!aggregate) {
                    out.write((files.get(i).getName() + ": file not found" + ls).getBytes(StandardCharsets.UTF_8));
                }
                continue;
            }
            if (files.get(i).isHidden()) {
                if (!aggregate) {
                    out.write((files.get(i).getName() + ": file not available" + ls).getBytes(StandardCharsets.UTF_8));
                }
                continue;
            }
            try (Scanner scanner = new Scanner(files.get(i))) {
                while (scanner.hasNextLine()) {
                    String str = scanner.nextLine();
                    String[] words = parse(str.trim());

                    for (int j = 0; j < words.length; ++j) {
                        if (mapOfWords.get(words[j]) == null) {
                            mapOfWords.put(words[j], 1);
                        } else {
                            mapOfWords.put(words[j], mapOfWords.get(words[j]) + 1);
                        }
                    }
                }
            }

            if (!aggregate) {
                if (mapOfWords.isEmpty()) {
                    out.write(("file " + files.get(i).getName() + " is empty" + ls).getBytes(StandardCharsets.UTF_8));
                } else {
                    out.write((files.get(i).getName() + ":" + ls).getBytes(StandardCharsets.UTF_8));
                    printMap(mapOfWords, out);
                    mapOfWords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                }

            }
        }
        if (aggregate) {
            printMap(mapOfWords, out);
        }
    }

    private void printMap(Map<String, Integer> map, OutputStream out) {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            try {
                out.write((entry.getKey() + " " + entry.getValue() + ls).getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                System.err.println(e);
            }
        }
    }

    private String[] parse(String s) {
        List<String> listOfWords = new ArrayList<>();
        char prevCh = ' ';
        StringBuilder currentString = new StringBuilder("");
        for (int i = 0; i < s.length(); ++i) {
            char ch = s.charAt(i);
            if (Character.isLetterOrDigit(ch) && (Character.isLetterOrDigit(prevCh) || Character.isSpaceChar(prevCh))) {
                currentString.append(ch);
                prevCh = ch;
                continue;
            }
            if (Character.isLetterOrDigit(ch) && prevCh == '-') {
                if (!currentString.equals("")) {
                    currentString.append("-");
                    currentString.append(ch);
                    prevCh = ch;
                } else {
                    currentString.append(ch);
                    prevCh = ch;
                }
                continue;
            }
            if (ch == '-') {
                if (prevCh == '-' && !currentString.toString().equals("")) {
                    listOfWords.add(currentString.toString());
                    currentString = new StringBuilder("");
                }
                prevCh = ch;
                continue;
            }
            if (!currentString.toString().equals("")) {
                listOfWords.add(currentString.toString());
                currentString = new StringBuilder("");
            }
        }
        if (!currentString.toString().equals("")) {
            listOfWords.add(currentString.toString());
        }

        String[] simpleArray = new String[listOfWords.size()];
        return listOfWords.toArray(simpleArray);
    }
}
